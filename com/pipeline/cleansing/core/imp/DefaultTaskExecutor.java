package com.pipeline.cleansing.core.impl;

import com.pipeline.cleansing.core.*;
import com.pipeline.cleansing.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务执行器默认实现。
 * 使用工作窃取线程池实现任务级并行，驱动算子管道执行清洗计算。
 */
public class DefaultTaskExecutor implements TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskExecutor.class);

    private final FunctionManager functionManager;
    private final TaskManager taskManager;
    private final ResourceManager resourceManager;
    private final DataStorage dataStorage;
    private final CacheManager cacheManager;

    /** 工作窃取线程池，自动平衡各线程负载 */
    private final ForkJoinPool workerPool;

    /** 单任务最大执行时长（毫秒），默认400ms（微批间隔的80%） */
    private final long taskTimeoutMs;

    /** 当前正在执行的任务集合，用于监控和重复执行检测 */
    private final ConcurrentHashMap<String, Future<?>> runningTasks = new ConcurrentHashMap<>();

    /** 执行统计 */
    private final AtomicInteger totalExecuted = new AtomicInteger(0);
    private final AtomicInteger totalFailed = new AtomicInteger(0);

    public DefaultTaskExecutor(FunctionManager functionManager,
                               TaskManager taskManager,
                               ResourceManager resourceManager,
                               DataStorage dataStorage,
                               CacheManager cacheManager,
                               int parallelism,
                               long taskTimeoutMs) {
        this.functionManager = functionManager;
        this.taskManager = taskManager;
        this.resourceManager = resourceManager;
        this.dataStorage = dataStorage;
        this.cacheManager = cacheManager;
        this.taskTimeoutMs = taskTimeoutMs;

        this.workerPool = new ForkJoinPool(
                parallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                (t, e) -> log.error("Uncaught exception in worker thread {}: {}",
                        t.getName(), e.getMessage(), e),
                true  // asyncMode=true，适合事件驱动型任务
        );

        log.info("TaskExecutor initialized. Parallelism: {}, Timeout: {}ms",
                parallelism, taskTimeoutMs);
    }

    @Override
    public void checkTaskStatus() {
        // 检查是否有上一轮仍在运行的任务
        runningTasks.entrySet().removeIf(entry -> {
            if (entry.getValue().isDone()) {
                return true;
            }
            log.warn("Task '{}' from previous batch is still running.", entry.getKey());
            return false;
        });
    }

    @Override
    public boolean checkTaskInputData(Task task) {
        String inputPointId = task.getConfig().getInputPointId();

        // 检查缓存中是否有该测点数据
        TimeSeriesData cached = cacheManager.getLatestData(inputPointId);
        if (cached == null || cached.isEmpty()) {
            // 回查存储层
            TimeRange recent = new TimeRange(
                    new Timestamp(System.currentTimeMillis() - task.getConfig().getLookbackSeconds() * 1000L),
                    new Timestamp(System.currentTimeMillis())
            );
            TimeSeriesData stored = dataStorage.queryTimeSeriesData(inputPointId, recent);
            if (stored == null || stored.isEmpty()) {
                log.debug("No input data available for task '{}', point '{}'",
                        task.getTaskId(), inputPointId);
                return false;
            }
            // 将查到的数据加载到缓存
            cacheManager.loadData(inputPointId, stored);
        }

        return true;
    }

    @Override
    public boolean checkOperatorParameters(Task task) {
        List<OperatorConfig> pipeline = task.getConfig().getOperatorPipeline();
        for (OperatorConfig opConfig : pipeline) {
            ValidationResult result = functionManager.validateFunction(
                    opConfig.getFunctionId(), opConfig.getParameters());
            if (!result.isValid()) {
                log.error("Parameter validation failed for task '{}', operator '{}': {}",
                        task.getTaskId(), opConfig.getFunctionId(), result.getErrors());
                task.setLastErrorMessage("Operator parameter validation failed: "
                        + result.getErrors());
                return false;
            }
        }
        return true;
    }

    @Override
    public void executeTask(Task task) {
        String taskId = task.getTaskId();

        // 重复执行检测
        if (runningTasks.containsKey(taskId)) {
            log.warn("Task '{}' is already running, skipping.", taskId);
            return;
        }

        // 提交到线程池执行
        Future<?> future = workerPool.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                taskManager.updateTaskStatus(taskId, TaskStatus.RUNNING);
                executePipeline(task);
                taskManager.updateTaskStatus(taskId, TaskStatus.COMPLETED);
                totalExecuted.incrementAndGet();

                long elapsed = System.currentTimeMillis() - startTime;
                log.debug("Task '{}' completed in {}ms", taskId, elapsed);

            } catch (Exception e) {
                totalFailed.incrementAndGet();
                task.setLastErrorMessage(e.getMessage());
                taskManager.updateTaskStatus(taskId, TaskStatus.FAILED);
                log.error("Task '{}' failed: {}", taskId, e.getMessage(), e);

                // 判断是否需要标记为故障态
                if (task.getFailureCount() >= 3) {
                    log.error("Task '{}' has failed {} times consecutively, marking as FAULTED.",
                            taskId, task.getFailureCount());
                    taskManager.updateTaskStatus(taskId, TaskStatus.FAULTED);
                }
            } finally {
                runningTasks.remove(taskId);
                resourceManager.releaseResourcesForTask(taskId);
            }
        });

        runningTasks.put(taskId, future);
    }

    @Override
    public void monitorExecution() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Future<?>> entry : runningTasks.entrySet()) {
            Future<?> future = entry.getValue();
            if (!future.isDone()) {
                // 超时检测由资源管理器处理
                // 此处仅记录长时间运行的任务
                log.debug("Task '{}' is still executing.", entry.getKey());
            }
        }
    }

    /**
     * 执行算子管道：按顺序串联执行各算子
     */
    private void executePipeline(Task task) {
        List<OperatorConfig> pipeline = task.getConfig().getOperatorPipeline();
        // 按order排序
        pipeline.sort(java.util.Comparator.comparingInt(OperatorConfig::getOrder));

        String inputPointId = task.getConfig().getInputPointId();
        String outputPointId = task.getConfig().getOutputPointId();

        // 创建管道执行上下文链
        // 第一个算子从原始输入读取，后续算子从前一个算子的输出读取
        DefaultOperatorContext previousContext = null;

        for (int i = 0; i < pipeline.size(); i++) {
            OperatorConfig opConfig = pipeline.get(i);
            UFunction function = functionManager.getFunction(opConfig.getFunctionId());

            if (function == null) {
                throw new RuntimeException("Function not found: " + opConfig.getFunctionId());
            }

            boolean isLast = (i == pipeline.size() - 1);

            DefaultOperatorContext context = new DefaultOperatorContext(
                    inputPointId,
                    outputPointId,
                    opConfig.getParameters(),
                    cacheManager,
                    dataStorage,
                    previousContext,
                    isLast
            );

            // 执行算子
            function.execute(context);

            previousContext = context;
        }
    }

    /** 获取执行统计 */
    public int getTotalExecuted() { return totalExecuted.get(); }
    public int getTotalFailed() { return totalFailed.get(); }
    public int getActiveTaskCount() { return runningTasks.size(); }
}
