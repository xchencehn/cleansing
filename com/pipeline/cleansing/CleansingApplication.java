package com.pipeline.cleansing;

import com.pipeline.cleansing.core.*;
import com.pipeline.cleansing.core.impl.*;
import com.pipeline.cleansing.operators.*;
import com.pipeline.cleansing.storage.SQLiteDataStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 系统启动引导类。
 * 一条命令完成全部初始化：创建存储、注册算子、加载任务、启动调度。
 *
 * 用法：java -jar pipeline-cleansing.jar [配置文件路径]
 */
public class CleansingApplication {

    private static final Logger log = LoggerFactory.getLogger(CleansingApplication.class);

    private DefaultEnvironment environment;
    private SQLiteDataStorage dataStorage;
    private CacheManager cacheManager;

    public void start(AppConfig config) {
        log.info("=== Pipeline IoT Time Series Data Cleansing Platform ===");
        log.info("Starting with config: {}", config);

        // 1. 初始化存储层
        dataStorage = new SQLiteDataStorage(
                config.getStorageRoot(),
                config.getTimeWindowMs()
        );

        // 2. 初始化缓存管理器
        cacheManager = new CacheManager(
                config.getCacheDefaultTTL(),
                config.getCacheMaxPoints(),
                config.getWriteBackQueueSize(),
                config.getWriteBackRateLimit()
        );

        // 设置写回回调：缓存淘汰的数据写入SQLite
        cacheManager.setWriteBackCallback((pointId, data) -> {
            for (var dp : data.getDataPoints()) {
                com.pipeline.cleansing.model.PointData pd = new com.pipeline.cleansing.model.PointData();
                pd.setTag(pointId);
                dataStorage.writeTimeSeriesPoint(pointId, dp);
            }
        });

        // 3. 初始化资源管理器
        DefaultResourceManager resourceManager = new DefaultResourceManager(
                config.getMemoryWarningThreshold(),
                config.getMemoryCriticalThreshold(),
                config.getCpuWarningThreshold()
        );
        resourceManager.setCacheManager(cacheManager);

        // 4. 初始化算子管理器并注册预置算子
        DefaultFunctionManager functionManager = new DefaultFunctionManager();
        registerBuiltinOperators(functionManager);

        // 5. 初始化任务管理器
        DefaultTaskManager taskManager = new DefaultTaskManager();

        // 6. 初始化任务执行器
        DefaultTaskExecutor taskExecutor = new DefaultTaskExecutor(
                functionManager,
                taskManager,
                resourceManager,
                dataStorage,
                cacheManager,
                config.getWorkerParallelism(),
                config.getTaskTimeoutMs()
        );

        // 7. 组装并启动运行时环境
        environment = DefaultEnvironment.initialize();
        environment.setFunctionManager(functionManager)
                .setTaskManager(taskManager)
                .setResourceManager(resourceManager)
                .setDataStorage(dataStorage)
                .setTaskExecutor(taskExecutor)
                .setMicroBatchIntervalMs(config.getMicroBatchIntervalMs());

        // 注册JVM关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, performing graceful shutdown...");
            shutdown();
        }, "shutdown-hook"));

        environment.start();

        log.info("=== Platform started successfully ===");
    }

    public void shutdown() {
        if (environment != null) {
            environment.shutdown();
        }
        if (cacheManager != null) {
            cacheManager.shutdown();
        }
        if (dataStorage != null) {
            dataStorage.shutdown();
        }
        log.info("=== Platform shut down ===");
    }

    /**
     * 注册系统预置的四类算子
     */
    private void registerBuiltinOperators(DefaultFunctionManager functionManager) {
        functionManager.registerFunction("threshold_detection", new ThresholdDetectionOperator());
        functionManager.registerFunction("interpolation_fill", new InterpolationFillOperator());
        functionManager.registerFunction("smoothing_filter", new SmoothingFilterOperator());
        functionManager.registerFunction("statistical_detection", new StatisticalDetectionOperator());

        log.info("Registered {} built-in operators.", functionManager.getAllFunctions().size());
    }

    /**
     * 应用入口
     */
    public static void main(String[] args) {
        String configPath = (args.length > 0) ? args[0] : "config/application.yml";

        AppConfig config = AppConfig.load(configPath);
        CleansingApplication app = new CleansingApplication();
        app.start(config);
    }
}
