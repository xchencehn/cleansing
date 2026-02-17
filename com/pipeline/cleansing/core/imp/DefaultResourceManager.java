package com.pipeline.cleansing.core.impl;

import com.pipeline.cleansing.core.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 资源管理器默认实现。
 * 监控CPU、内存、缓存使用情况，在资源异常时主动干预。
 */
public class DefaultResourceManager implements ResourceManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultResourceManager.class);

    /** 内存使用预警阈值（堆内存占比） */
    private final double memoryWarningThreshold;
    /** 内存使用紧急阈值，超过此值触发强制淘汰 */
    private final double memoryCriticalThreshold;
    /** CPU使用预警阈值 */
    private final double cpuWarningThreshold;

    /** 任务资源占用追踪：taskId -> 资源快照 */
    private final ConcurrentHashMap<String, TaskResourceSnapshot> taskResources = new ConcurrentHashMap<>();

    private final MemoryMXBean memoryBean;
    private final OperatingSystemMXBean osBean;

    /** 关联的缓存管理器，用于在资源紧张时触发缓存淘汰 */
    private CacheManager cacheManager;

    public DefaultResourceManager(double memoryWarningThreshold,
                                  double memoryCriticalThreshold,
                                  double cpuWarningThreshold) {
        this.memoryWarningThreshold = memoryWarningThreshold;
        this.memoryCriticalThreshold = memoryCriticalThreshold;
        this.cpuWarningThreshold = cpuWarningThreshold;
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.osBean = ManagementFactory.getOperatingSystemMXBean();
    }

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public void allocateResources() {
        log.info("Resource allocation initialized. Memory warning: {}%, critical: {}%, CPU warning: {}%",
                memoryWarningThreshold * 100, memoryCriticalThreshold * 100, cpuWarningThreshold * 100);

        // 预检系统资源是否满足最低要求
        long maxMemory = Runtime.getRuntime().maxMemory();
        long minRequired = 512 * 1024 * 1024L; // 最低512MB
        if (maxMemory < minRequired) {
            log.warn("Available heap memory {}MB is below recommended minimum {}MB",
                    maxMemory / (1024 * 1024), minRequired / (1024 * 1024));
        }
    }

    @Override
    public void releaseResourcesForTask(String taskId) {
        TaskResourceSnapshot removed = taskResources.remove(taskId);
        if (removed != null) {
            log.debug("Released resources for task '{}'. Peak memory: {}KB, CPU time: {}ms",
                    taskId, removed.peakMemoryKB, removed.cpuTimeMs);
        }
    }

    @Override
    public void monitorTaskResources() {
        for (Map.Entry<String, TaskResourceSnapshot> entry : taskResources.entrySet()) {
            TaskResourceSnapshot snapshot = entry.getValue();
            long elapsed = System.currentTimeMillis() - snapshot.startTimeMs;

            if (elapsed > snapshot.timeoutMs) {
                log.warn("Task '{}' has been running for {}ms, exceeding timeout of {}ms",
                        entry.getKey(), elapsed, snapshot.timeoutMs);
                // 超时任务由terminateTask处理
            }
        }
    }

    @Override
    public void monitorSystemResources() {
        // 内存监控
        long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();
        long maxMemory = memoryBean.getHeapMemoryUsage().getMax();
        double memoryUsage = (double) usedMemory / maxMemory;

        if (memoryUsage > memoryCriticalThreshold) {
            log.error("CRITICAL: Memory usage at {:.1f}%, triggering emergency cache eviction.",
                    memoryUsage * 100);
            if (cacheManager != null) {
                cacheManager.forceEvict();
            }
        } else if (memoryUsage > memoryWarningThreshold) {
            log.warn("Memory usage at {:.1f}%, approaching critical threshold.",
                    memoryUsage * 100);
            if (cacheManager != null) {
                cacheManager.accelerateEviction();
            }
        }

        // CPU监控
        double cpuLoad = osBean.getSystemLoadAverage();
        int processors = osBean.getAvailableProcessors();
        double normalizedLoad = cpuLoad / processors;

        if (normalizedLoad > cpuWarningThreshold) {
            log.warn("CPU load average {:.2f} (normalized: {:.2f}), exceeding warning threshold.",
                    cpuLoad, normalizedLoad);
        }
    }

    @Override
    public boolean terminateTask(String taskId) {
        TaskResourceSnapshot snapshot = taskResources.get(taskId);
        if (snapshot == null) {
            log.warn("Cannot terminate task '{}': not tracked in resource manager.", taskId);
            return false;
        }

        log.warn("Terminating task '{}' due to resource violation.", taskId);
        // 实际终止由TaskExecutor中的Future.cancel()执行
        // 此处标记并通知
        snapshot.terminated = true;
        releaseResourcesForTask(taskId);
        return true;
    }

    /** 注册任务资源追踪 */
    public void trackTask(String taskId, long timeoutMs) {
        taskResources.put(taskId, new TaskResourceSnapshot(timeoutMs));
    }

    /** 获取当前内存使用率 */
    public double getMemoryUsage() {
        return (double) memoryBean.getHeapMemoryUsage().getUsed()
                / memoryBean.getHeapMemoryUsage().getMax();
    }

    /**
     * 任务资源快照
     */
    static class TaskResourceSnapshot {
        final long startTimeMs;
        final long timeoutMs;
        long peakMemoryKB;
        long cpuTimeMs;
        volatile boolean terminated;

        TaskResourceSnapshot(long timeoutMs) {
            this.startTimeMs = System.currentTimeMillis();
            this.timeoutMs = timeoutMs;
            this.terminated = false;
        }
    }
}
