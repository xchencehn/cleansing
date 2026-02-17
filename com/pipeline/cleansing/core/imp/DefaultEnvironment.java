package com.pipeline.cleansing.core.impl;

import com.pipeline.cleansing.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 运行时环境默认实现。
 * 管理所有核心组件的生命周期，控制系统启停。
 */
public class DefaultEnvironment implements Environment {

    private static final Logger log = LoggerFactory.getLogger(DefaultEnvironment.class);

    private FunctionManager functionManager;
    private TaskManager taskManager;
    private ResourceManager resourceManager;
    private DataStorage dataStorage;
    private TaskExecutor taskExecutor;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread schedulerThread;

    /** 微批调度间隔（毫秒），默认500ms */
    private long microBatchIntervalMs = 500L;

    private DefaultEnvironment() {}

    public static DefaultEnvironment initialize() {
        log.info("Initializing pipeline cleansing environment...");
        return new DefaultEnvironment();
    }

    @Override
    public Environment setFunctionManager(FunctionManager functionManager) {
        this.functionManager = functionManager;
        return this;
    }

    @Override
    public Environment setTaskManager(TaskManager taskManager) {
        this.taskManager = taskManager;
        return this;
    }

    @Override
    public Environment setResourceManager(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
        return this;
    }

    @Override
    public Environment setDataStorage(DataStorage dataStorage) {
        this.dataStorage = dataStorage;
        return this;
    }

    @Override
    public Environment setTaskExecutor(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
        return this;
    }

    public Environment setMicroBatchIntervalMs(long intervalMs) {
        if (intervalMs < 100 || intervalMs > 5000) {
            throw new IllegalArgumentException(
                "Micro-batch interval must be between 100ms and 5000ms, got: " + intervalMs);
        }
        this.microBatchIntervalMs = intervalMs;
        return this;
    }

    @Override
    public void start() {
        validateComponents();

        if (!running.compareAndSet(false, true)) {
            log.warn("Environment is already running, ignoring duplicate start.");
            return;
        }

        log.info("Starting pipeline cleansing environment...");
        log.info("Micro-batch interval: {}ms", microBatchIntervalMs);

        // 按依赖顺序初始化：存储 → 资源 → 算子 → 任务 → 执行器
        log.info("Initializing DataStorage...");
        // DataStorage在构造时已初始化（SQLite文件创建等）

        log.info("Initializing ResourceManager...");
        resourceManager.allocateResources();

        log.info("Initializing FunctionManager with {} registered operators.",
                functionManager.getAllFunctions().size());

        log.info("Initializing TaskManager with {} registered tasks.",
                taskManager.getAllTasks().size());

        // 启动微批调度循环
        schedulerThread = new Thread(this::microBatchLoop, "micro-batch-scheduler");
        schedulerThread.setDaemon(false);
        schedulerThread.start();

        log.info("Pipeline cleansing environment started successfully.");
    }

    @Override
    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            log.warn("Environment is not running, ignoring shutdown.");
            return;
        }

        log.info("Shutting down pipeline cleansing environment...");

        // 停止调度循环
        if (schedulerThread != null) {
            schedulerThread.interrupt();
            try {
                schedulerThread.join(microBatchIntervalMs * 3);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for scheduler thread to finish.");
            }
        }

        // 按与启动相反的顺序关闭
        log.info("Flushing pending data to storage...");
        // 触发缓存全量写回

        log.info("Pipeline cleansing environment shut down successfully.");
    }

    /**
     * 微批调度主循环
     */
    private void microBatchLoop() {
        log.info("Micro-batch scheduler started.");
        while (running.get()) {
            long batchStartTime = System.currentTimeMillis();
            try {
                executeMicroBatch();
            } catch (Exception e) {
                log.error("Error during micro-batch execution", e);
            }

            // 计算本轮耗时，补齐到微批间隔
            long elapsed = System.currentTimeMillis() - batchStartTime;
            long sleepTime = microBatchIntervalMs - elapsed;
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else {
                log.warn("Micro-batch execution took {}ms, exceeding interval of {}ms",
                        elapsed, microBatchIntervalMs);
            }
        }
        log.info("Micro-batch scheduler stopped.");
    }

    /**
     * 单轮微批执行
     */
    private void executeMicroBatch() {
        // 1. 系统状态检查
        taskExecutor.checkTaskStatus();

        // 2. 监控系统资源
        resourceManager.monitorSystemResources();
        resourceManager.monitorTaskResources();

        // 3. 获取活跃任务并执行
        var activeTasks = taskManager.getActiveTasks();
        if (activeTasks.isEmpty()) {
            return;
        }

        for (var task : activeTasks) {
            try {
                // 前置检查
                if (!taskExecutor.checkTaskInputData(task)) {
                    continue;
                }
                if (!taskExecutor.checkOperatorParameters(task)) {
                    continue;
                }
                // 执行清洗
                taskExecutor.executeTask(task);
            } catch (Exception e) {
                log.error("Task {} execution failed: {}", task.getTaskId(), e.getMessage(), e);
                taskManager.updateTaskStatus(task.getTaskId(),
                        com.pipeline.cleansing.model.TaskStatus.FAILED);
            }
        }

        // 4. 执行监控
        taskExecutor.monitorExecution();
    }

    private void validateComponents() {
        if (functionManager == null) throw new IllegalStateException("FunctionManager is required");
        if (taskManager == null) throw new IllegalStateException("TaskManager is required");
        if (resourceManager == null) throw new IllegalStateException("ResourceManager is required");
        if (dataStorage == null) throw new IllegalStateException("DataStorage is required");
        if (taskExecutor == null) throw new IllegalStateException("TaskExecutor is required");
    }

    // ---- Getter methods for inter-component access ----

    public FunctionManager getFunctionManager() { return functionManager; }
    public TaskManager getTaskManager() { return taskManager; }
    public ResourceManager getResourceManager() { return resourceManager; }
    public DataStorage getDataStorage() { return dataStorage; }
    public TaskExecutor getTaskExecutor() { return taskExecutor; }
    public boolean isRunning() { return running.get(); }
}
