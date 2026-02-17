package com.pipeline.cleansing.core.impl;

import com.pipeline.cleansing.model.DataPoint;
import com.pipeline.cleansing.model.TimeRange;
import com.pipeline.cleansing.model.TimeSeriesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 多级缓存管理器。
 *
 * 实现文档第5.4节描述的四层缓存机制：
 * 1. 点位级缓存生命周期管理（TTL）
 * 2. 时间窗口滑动淘汰
 * 3. 异步标记—写回
 * 4. 计算结果缓存复用
 */
public class CacheManager {

    private static final Logger log = LoggerFactory.getLogger(CacheManager.class);

    // ---- 第一层：点位级缓存 ----

    /** 点位缓存实体 */
    private final ConcurrentHashMap<String, PointCacheEntry> pointCaches = new ConcurrentHashMap<>();

    /** TTL初始值（微批周期数） */
    private final int defaultTTL;

    /** 缓存容量上限（最大测点数） */
    private final int maxCachePoints;

    // ---- 第三层：异步写回队列 ----

    /** 待写回磁盘的数据队列 */
    private final BlockingQueue<WriteBackEntry> writeBackQueue;

    /** 异步写回线程 */
    private final Thread writeBackThread;
    private volatile boolean running = true;

    /** 写回速率控制（每秒最大写回批次） */
    private final int writeBackRateLimit;

    // ---- 第四层：计算结果缓存 ----

    /** 计算结果缓存：outputPointId -> 最近一批结果 */
    private final ConcurrentHashMap<String, TimeSeriesData> resultCache = new ConcurrentHashMap<>();

    /** 数据依赖关系：sourcePointId -> Set<outputPointId> */
    private final ConcurrentHashMap<String, Set<String>> resultDependencies = new ConcurrentHashMap<>();

    /** 写回回调接口 */
    private WriteBackCallback writeBackCallback;

    public CacheManager(int defaultTTL, int maxCachePoints,
                        int writeBackQueueSize, int writeBackRateLimit) {
        this.defaultTTL = defaultTTL;
        this.maxCachePoints = maxCachePoints;
        this.writeBackRateLimit = writeBackRateLimit;
        this.writeBackQueue = new LinkedBlockingQueue<>(writeBackQueueSize);

        // 启动异步写回线程
        this.writeBackThread = new Thread(this::writeBackLoop, "cache-writeback");
        this.writeBackThread.setDaemon(true);
        this.writeBackThread.start();

        log.info("CacheManager initialized. TTL: {}, MaxPoints: {}, WriteBackQueue: {}",
                defaultTTL, maxCachePoints, writeBackQueueSize);
    }

    public void setWriteBackCallback(WriteBackCallback callback) {
        this.writeBackCallback = callback;
    }

    // ==================== 数据读取 ====================

    /**
     * 获取指定测点在时间范围内的缓存数据
     */
    public TimeSeriesData getData(String pointId, TimeRange timeRange) {
        PointCacheEntry entry = pointCaches.get(pointId);
        if (entry == null) {
            return null;
        }

        entry.resetTTL(defaultTTL);  // 访问时重置TTL

        if (timeRange == null) {
            return entry.getAllData();
        }
        return entry.getData(timeRange);
    }

    /**
     * 获取指定测点最新的缓存数据
     */
    public TimeSeriesData getLatestData(String pointId) {
        PointCacheEntry entry = pointCaches.get(pointId);
        if (entry == null) {
            return null;
        }
        return entry.getAllData();
    }

    // ==================== 数据写入 ====================

    /**
     * 加载数据到缓存（从存储回查后填充）
     */
    public void loadData(String pointId, TimeSeriesData data) {
        PointCacheEntry entry = pointCaches.computeIfAbsent(pointId,
                k -> new PointCacheEntry(k, defaultTTL));
        for (DataPoint dp : data.getDataPoints()) {
            entry.addDataPoint(dp);
        }
        checkCapacity();
    }

    /**
     * 写入计算结果（管道最后一个算子调用）
     */
    public void putResult(String outputPointId, DataPoint point) {
        // 写入点位缓存
        PointCacheEntry entry = pointCaches.computeIfAbsent(outputPointId,
                k -> new PointCacheEntry(k, defaultTTL));
        entry.addDataPoint(point);

        // 更新结果缓存
        resultCache.computeIfAbsent(outputPointId, k -> new TimeSeriesData(k))
                .addDataPoint(point);

        checkCapacity();
    }

    /**
     * 重置测点TTL（数据被访问时调用）
     */
    public void resetTTL(String pointId) {
        PointCacheEntry entry = pointCaches.get(pointId);
        if (entry != null) {
            entry.resetTTL(defaultTTL);
        }
    }

    // ==================== 第一层：TTL生命周期管理 ====================

    /**
     * 每个微批周期结束后调用，递减未被访问的测点TTL
     */
    public void tickTTL() {
        List<String> expired = new ArrayList<>();

        pointCaches.forEach((pointId, entry) -> {
            int remaining = entry.decrementTTL();
            if (remaining <= 0) {
                expired.add(pointId);
            }
        });

        for (String pointId : expired) {
            PointCacheEntry removed = pointCaches.remove(pointId);
            if (removed != null) {
                // 将待淘汰的数据加入写回队列
                enqueueWriteBack(pointId, removed.getAllData());
                log.debug("Point '{}' cache expired (TTL=0), enqueued for write-back.", pointId);
            }
        }
    }

    // ==================== 第二层：时间窗口滑动淘汰 ====================

    /**
     * 淘汰指定测点中早于给定时间戳的数据
     */
    public void evictBefore(String pointId, Timestamp minTimestamp) {
        PointCacheEntry entry = pointCaches.get(pointId);
        if (entry == null) return;

        TimeSeriesData evicted = entry.evictBefore(minTimestamp);
        if (evicted != null && !evicted.isEmpty()) {
            enqueueWriteBack(pointId, evicted);
        }
    }

    // ==================== 第三层：异步写回 ====================

    private void enqueueWriteBack(String pointId, TimeSeriesData data) {
        if (data == null || data.isEmpty()) return;

        WriteBackEntry wbEntry = new WriteBackEntry(pointId, data);
        if (!writeBackQueue.offer(wbEntry)) {
            log.warn("Write-back queue is full, performing synchronous write for point '{}'.", pointId);
            // 队列满时同步写入，防止数据丢失
            if (writeBackCallback != null) {
                writeBackCallback.onWriteBack(pointId, data);
            }
        }
    }

    /**
     * 异步写回线程主循环，以恒定速率消费写回队列
     */
    private void writeBackLoop() {
        log.info("Write-back thread started.");
        long intervalMs = 1000L / writeBackRateLimit;

        while (running) {
            try {
                WriteBackEntry entry = writeBackQueue.poll(intervalMs, TimeUnit.MILLISECONDS);
                if (entry != null && writeBackCallback != null) {
                    try {
                        writeBackCallback.onWriteBack(entry.pointId, entry.data);
                    } catch (Exception e) {
                        log.error("Write-back failed for point '{}': {}",
                                entry.pointId, e.getMessage(), e);
                        // 失败的数据重新入队（有限重试）
                        if (entry.retryCount < 3) {
                            entry.retryCount++;
                            writeBackQueue.offer(entry);
                        } else {
                            log.error("Write-back for point '{}' failed after 3 retries, data dropped.",
                                    entry.pointId);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("Write-back thread stopped.");
    }

    // ==================== 第四层：计算结果缓存复用 ====================

    /**
     * 获取缓存的计算结果（供共享上游数据的下游任务使用）
     */
    public TimeSeriesData getCachedResult(String outputPointId) {
        return resultCache.get(outputPointId);
    }

    /**
     * 注册结果依赖关系
     */
    public void registerResultDependency(String sourcePointId, String outputPointId) {
        resultDependencies.computeIfAbsent(sourcePointId,
                k -> ConcurrentHashMap.newKeySet()).add(outputPointId);
    }

    /**
     * 源数据更新时，使依赖的计算结果缓存失效
     */
    public void invalidateResults(String sourcePointId) {
        Set<String> dependents = resultDependencies.get(sourcePointId);
        if (dependents != null) {
            for (String outputId : dependents) {
                resultCache.remove(outputId);
            }
        }
    }

    // ==================== 容量管理 ====================

    private void checkCapacity() {
        if (pointCaches.size() > maxCachePoints) {
            // 淘汰TTL最低的测点
            evictLowestTTL(pointCaches.size() - maxCachePoints);
        }
    }

    private void evictLowestTTL(int count) {
        pointCaches.entrySet().stream()
                .sorted(Comparator.comparingInt(e -> e.getValue().getCurrentTTL()))
                .limit(count)
                .map(Map.Entry::getKey)
                .forEach(pointId -> {
                    PointCacheEntry removed = pointCaches.remove(pointId);
                    if (removed != null) {
                        enqueueWriteBack(pointId, removed.getAllData());
                        log.debug("Evicted point '{}' (TTL={}) due to capacity limit.",
                                pointId, removed.getCurrentTTL());
                    }
                });
    }

    /** 资源管理器调用：加速淘汰 */
    public void accelerateEviction() {
        int toEvict = Math.max(1, pointCaches.size() / 10);
        evictLowestTTL(toEvict);
        log.warn("Accelerated eviction: {} points evicted.", toEvict);
    }

    /** 资源管理器调用：紧急全量淘汰 */
    public void forceEvict() {
        int toEvict = Math.max(1, pointCaches.size() / 3);
        evictLowestTTL(toEvict);
        log.error("Force eviction: {} points evicted.", toEvict);
    }

    public void shutdown() {
        running = false;
        writeBackThread.interrupt();
        // 将缓存中剩余数据全部写回
        pointCaches.forEach((pointId, entry) -> {
            if (writeBackCallback != null) {
                writeBackCallback.onWriteBack(pointId, entry.getAllData());
            }
        });
        pointCaches.clear();
        resultCache.clear();
        log.info("CacheManager shut down, all cached data flushed.");
    }

    public int getCachedPointCount() { return pointCaches.size(); }
    public int getWriteBackQueueSize() { return writeBackQueue.size(); }

    // ==================== 内部数据结构 ====================

    /**
     * 单个测点的缓存实体
     */
    static class PointCacheEntry {
        private final String pointId;
        private final AtomicInteger ttl;
        /** 有序数据点列表（按时间戳升序） */
        private final ConcurrentSkipListMap<Long, DataPoint> dataPoints = new ConcurrentSkipListMap<>();

        PointCacheEntry(String pointId, int initialTTL) {
            this.pointId = pointId;
            this.ttl = new AtomicInteger(initialTTL);
        }

        void addDataPoint(DataPoint dp) {
            dataPoints.put(dp.getTimestamp().getTime(), dp);
        }

        TimeSeriesData getAllData() {
            TimeSeriesData result = new TimeSeriesData(pointId);
            dataPoints.values().forEach(result::addDataPoint);
            return result;
        }

        TimeSeriesData getData(TimeRange range) {
            TimeSeriesData result = new TimeSeriesData(pointId);
            long from = range.getStart().getTime();
            long to = range.getEnd().getTime();
            dataPoints.subMap(from, true, to, true)
                    .values().forEach(result::addDataPoint);
            return result;
        }

        /**
         * 淘汰早于指定时间戳的数据，返回被淘汰的数据
         */
        TimeSeriesData evictBefore(Timestamp minTimestamp) {
            long threshold = minTimestamp.getTime();
            TimeSeriesData evicted = new TimeSeriesData(pointId);
            NavigableMap<Long, DataPoint> headMap = dataPoints.headMap(threshold, false);
            headMap.values().forEach(evicted::addDataPoint);
            headMap.clear();
            return evicted;
        }

        void resetTTL(int value) { ttl.set(value); }
        int decrementTTL() { return ttl.decrementAndGet(); }
        int getCurrentTTL() { return ttl.get(); }
        int size() { return dataPoints.size(); }
    }

    /**
     * 写回队列条目
     */
    static class WriteBackEntry {
        final String pointId;
        final TimeSeriesData data;
        int retryCount = 0;

        WriteBackEntry(String pointId, TimeSeriesData data) {
            this.pointId = pointId;
            this.data = data;
        }
    }

    /**
     * 写回回调接口
     */
    @FunctionalInterface
    public interface WriteBackCallback {
        void onWriteBack(String pointId, TimeSeriesData data);
    }
}
