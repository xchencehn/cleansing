package com.pipeline.cleansing.storage;

import com.pipeline.cleansing.core.DataStorage;
import com.pipeline.cleansing.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于SQLite的数据存储实现。
 *
 * 核心设计：
 * - 一点一表，消除写入锁竞争
 * - 按时间窗口分库（每个库对应一个SQLite文件）
 * - 时间戳B-tree索引
 * - 内置逢变才存压缩逻辑
 */
public class SQLiteDataStorage implements DataStorage {

    private static final Logger log = LoggerFactory.getLogger(SQLiteDataStorage.class);

    /** 存储根目录 */
    private final String storageRoot;

    /** 时间窗口大小（毫秒），默认1天 */
    private final long timeWindowMs;

    /** 测点元数据表的连接 */
    private Connection metaConnection;

    /** 数据库连接池：dbKey -> Connection */
    private final ConcurrentHashMap<String, Connection> connectionPool = new ConcurrentHashMap<>();

    /** 每个测点最后存储的数据点（逢变才存判断用） */
    private final ConcurrentHashMap<String, DataPoint> lastStoredPoints = new ConcurrentHashMap<>();

    /** 每个测点最后存储时间（最大静默间隔判断用） */
    private final ConcurrentHashMap<String, Long> lastStoredTimes = new ConcurrentHashMap<>();

    /** 测点元数据缓存 */
    private final ConcurrentHashMap<String, PointData> pointMetaCache = new ConcurrentHashMap<>();

    public SQLiteDataStorage(String storageRoot) {
        this(storageRoot, 24 * 60 * 60 * 1000L); // 默认1天一个库
    }

    public SQLiteDataStorage(String storageRoot, long timeWindowMs) {
        this.storageRoot = storageRoot;
        this.timeWindowMs = timeWindowMs;

        // 确保存储目录存在
        File dir = new File(storageRoot);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new RuntimeException("Failed to create storage directory: " + storageRoot);
        }

        initMetaDatabase();
        log.info("SQLiteDataStorage initialized. Root: {}, TimeWindow: {}ms", storageRoot, timeWindowMs);
    }

    // ==================== 测点元数据管理 ====================

    @Override
    public String addDataPoint(PointData pointData) {
        String tag = pointData.getTag();
        if (tag == null || tag.isBlank()) {
            throw new IllegalArgumentException("Point tag must not be null or blank");
        }

        try {
            String sql = "INSERT OR REPLACE INTO point_meta (tag, name, unit, range_low, range_high, "
                    + "pipeline_segment, deadband, max_silent_interval, retention_days) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = metaConnection.prepareStatement(sql)) {
                stmt.setString(1, tag);
                stmt.setString(2, pointData.getName());
                stmt.setString(3, pointData.getUnit());
                stmt.setDouble(4, pointData.getRangeLow());
                stmt.setDouble(5, pointData.getRangeHigh());
                stmt.setString(6, pointData.getPipelineSegment());
                stmt.setDouble(7, pointData.getDeadband());
                stmt.setInt(8, pointData.getMaxSilentInterval());
                stmt.setInt(9, pointData.getRetentionDays());
                stmt.executeUpdate();
            }

            pointMetaCache.put(tag, pointData);
            log.info("Point '{}' added/updated.", tag);
            return tag;

        } catch (SQLException e) {
            log.error("Failed to add point '{}': {}", tag, e.getMessage(), e);
            throw new RuntimeException("Failed to add data point", e);
        }
    }

    @Override
    public boolean deleteDataPoint(String tag) {
        try {
            String sql = "DELETE FROM point_meta WHERE tag = ?";
            try (PreparedStatement stmt = metaConnection.prepareStatement(sql)) {
                stmt.setString(1, tag);
                int affected = stmt.executeUpdate();
                if (affected > 0) {
                    pointMetaCache.remove(tag);
                    lastStoredPoints.remove(tag);
                    lastStoredTimes.remove(tag);
                    log.info("Point '{}' deleted.", tag);
                    return true;
                }
            }
            return false;
        } catch (SQLException e) {
            log.error("Failed to delete point '{}': {}", tag, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean updateDataPoint(String dataPointId, PointData newPointData) {
        newPointData.setTag(dataPointId);
        try {
            addDataPoint(newPointData);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public PointData queryDataPoint(String dataPointId) {
        PointData cached = pointMetaCache.get(dataPointId);
        if (cached != null) return cached;

        try {
            String sql = "SELECT * FROM point_meta WHERE tag = ?";
            try (PreparedStatement stmt = metaConnection.prepareStatement(sql)) {
                stmt.setString(1, dataPointId);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    PointData pd = mapResultSetToPointData(rs);
                    pointMetaCache.put(dataPointId, pd);
                    return pd;
                }
            }
            return null;
        } catch (SQLException e) {
            log.error("Failed to query point '{}': {}", dataPointId, e.getMessage(), e);
            return null;
        }
    }

    // ==================== 时序数据读写 ====================

    @Override
    public TimeSeriesData queryTimeSeriesData(String dataPointId, TimeRange timeRange) {
        TimeSeriesData result = new TimeSeriesData(dataPointId);
        String tableName = sanitizeTableName(dataPointId);

        // 确定涉及哪些分库
        long startMs = timeRange.getStart().getTime();
        long endMs = timeRange.getEnd().getTime();

        for (long windowStart = alignToWindow(startMs); windowStart <= endMs; windowStart += timeWindowMs) {
            String dbKey = getDbKey(windowStart);
            Connection conn = getOrCreateConnection(dbKey);
            if (conn == null) continue;

            try {
                // 检查表是否存在
                if (!tableExists(conn, tableName)) continue;

                String sql = "SELECT timestamp, value, quality FROM " + tableName
                        + " WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setLong(1, startMs);
                    stmt.setLong(2, endMs);
                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        DataPoint dp = new DataPoint(
                                new Timestamp(rs.getLong("timestamp")),
                                rs.getDouble("value"),
                                rs.getInt("quality")
                        );
                        result.addDataPoint(dp);
                    }
                }
            } catch (SQLException e) {
                log.error("Failed to query time series for point '{}' in db '{}': {}",
                        dataPointId, dbKey, e.getMessage(), e);
            }
        }

        return result;
    }

    @Override
    public boolean putDataPoint(PointData pointData) {
        // 此接口用于putDataPoint(PointData)的兼容，
        // 实际的时序数据写入通过writeTimeSeriesPoint完成
        return false;
    }

    /**
     * 写入单个时序数据点（含逢变才存逻辑）
     *
     * @return true表示实际写入了数据，false表示被死区过滤（非错误）
     */
    public boolean writeTimeSeriesPoint(String pointId, DataPoint newPoint) {
        // ---- 逢变才存判定 ----
        PointData meta = pointMetaCache.get(pointId);
        double deadband = (meta != null) ? meta.getDeadband() : 0.0;
        int maxSilentInterval = (meta != null) ? meta.getMaxSilentInterval() : 3600;

        DataPoint lastStored = lastStoredPoints.get(pointId);
        Long lastStoredTime = lastStoredTimes.get(pointId);
        long nowMs = newPoint.getTimestamp().getTime();

        boolean shouldStore = false;

        if (lastStored == null) {
            // 该测点第一条数据，必须存储
            shouldStore = true;
        } else {
            // 条件1：数值变化超过死区
            if (newPoint.getValue() instanceof Number && lastStored.getValue() instanceof Number) {
                double diff = Math.abs(
                        ((Number) newPoint.getValue()).doubleValue()
                        - ((Number) lastStored.getValue()).doubleValue());
                if (diff >= deadband) {
                    shouldStore = true;
                }
            } else {
                // 非数值类型，直接比较
                if (!Objects.equals(newPoint.getValue(), lastStored.getValue())) {
                    shouldStore = true;
                }
            }

            // 条件2：质量码变化
            if (newPoint.getQualityCode() != lastStored.getQualityCode()) {
                shouldStore = true;
            }

            // 条件3：超过最大静默间隔
            if (lastStoredTime != null
                    && (nowMs - lastStoredTime) >= maxSilentInterval * 1000L) {
                shouldStore = true;
            }
        }

        if (!shouldStore) {
            return false;
        }

        // ---- 实际写入 ----
        String tableName = sanitizeTableName(pointId);
        String dbKey = getDbKey(nowMs);
        Connection conn = getOrCreateConnection(dbKey);

        try {
            ensureTableExists(conn, tableName);

            String sql = "INSERT INTO " + tableName + " (timestamp, value, quality) VALUES (?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, nowMs);
                stmt.setDouble(2, ((Number) newPoint.getValue()).doubleValue());
                stmt.setInt(3, newPoint.getQualityCode());
                stmt.executeUpdate();
            }

            // 更新逢变才存状态
            lastStoredPoints.put(pointId, newPoint);
            lastStoredTimes.put(pointId, nowMs);

            return true;

        } catch (SQLException e) {
            log.error("Failed to write point '{}': {}", pointId, e.getMessage(), e);
            return false;
        }
    }

    // ==================== 内部工具方法 ====================

    private void initMetaDatabase() {
        try {
            String metaDbPath = storageRoot + File.separator + "meta.db";
            metaConnection = DriverManager.getConnection("jdbc:sqlite:" + metaDbPath);
            metaConnection.setAutoCommit(true);

            // 启用WAL模式
            try (Statement stmt = metaConnection.createStatement()) {
                stmt.execute("PRAGMA journal_mode=WAL");
                stmt.execute("PRAGMA synchronous=NORMAL");
            }

            // 创建元数据表
            String createSql = "CREATE TABLE IF NOT EXISTS point_meta ("
                    + "tag TEXT PRIMARY KEY, "
                    + "name TEXT, "
                    + "unit TEXT, "
                    + "range_low REAL, "
                    + "range_high REAL, "
                    + "pipeline_segment TEXT, "
                    + "deadband REAL DEFAULT 0.0, "
                    + "max_silent_interval INTEGER DEFAULT 3600, "
                    + "retention_days INTEGER DEFAULT 365)";
            try (Statement stmt = metaConnection.createStatement()) {
                stmt.execute(createSql);
            }

            log.info("Meta database initialized at: {}", metaDbPath);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize meta database", e);
        }
    }

    private Connection getOrCreateConnection(String dbKey) {
        return connectionPool.computeIfAbsent(dbKey, key -> {
            try {
                String dbPath = storageRoot + File.separator + key + ".db";
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                conn.setAutoCommit(true);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("PRAGMA journal_mode=WAL");
                    stmt.execute("PRAGMA synchronous=NORMAL");
                    stmt.execute("PRAGMA cache_size=10000");
                }
                return conn;
            } catch (SQLException e) {
                log.error("Failed to create connection for db '{}': {}", key, e.getMessage(), e);
                return null;
            }
        });
    }

    private void ensureTableExists(Connection conn, String tableName) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "timestamp INTEGER NOT NULL, "
                + "value REAL, "
                + "quality INTEGER DEFAULT 0, "
                + "PRIMARY KEY (timestamp))";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private boolean tableExists(Connection conn, String tableName) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?")) {
            stmt.setString(1, tableName);
            return stmt.executeQuery().next();
        } catch (SQLException e) {
            return false;
        }
    }

    /** 时间戳对齐到时间窗口起点 */
    private long alignToWindow(long timestampMs) {
        return (timestampMs / timeWindowMs) * timeWindowMs;
    }

    /** 根据时间戳生成数据库键名 */
    private String getDbKey(long timestampMs) {
        long windowStart = alignToWindow(timestampMs);
        return "ts_" + windowStart;
    }

    /** 将测点tag转为合法的SQLite表名 */
    private String sanitizeTableName(String pointId) {
        return "p_" + pointId.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private PointData mapResultSetToPointData(ResultSet rs) throws SQLException {
        PointData pd = new PointData();
        pd.setTag(rs.getString("tag"));
        pd.setName(rs.getString("name"));
        pd.setUnit(rs.getString("unit"));
        pd.setRangeLow(rs.getDouble("range_low"));
        pd.setRangeHigh(rs.getDouble("range_high"));
        pd.setPipelineSegment(rs.getString("pipeline_segment"));
        pd.setDeadband(rs.getDouble("deadband"));
        pd.setMaxSilentInterval(rs.getInt("max_silent_interval"));
        pd.setRetentionDays(rs.getInt("retention_days"));
        return pd;
    }

    /** 关闭所有连接 */
    public void shutdown() {
        connectionPool.values().forEach(conn -> {
            try { conn.close(); } catch (SQLException ignored) {}
        });
        connectionPool.clear();
        try { metaConnection.close(); } catch (SQLException ignored) {}
        log.info("SQLiteDataStorage shut down.");
    }

    private static final java.util.Objects Objects = null; // placeholder
}
