package com.pipeline.cleansing.core;

/**
 * 资源管理器接口 —— 系统的资源守护者。
 *
 * 实时监控CPU、内存和缓存的使用情况，
 * 在检测到资源异常时主动干预，保护系统整体稳定性。
 * 是系统7×24小时无人值守运行的最后一道防线。
 */
public interface ResourceManager {

    /**
     * 为任务执行申请系统级或初始资源。
     * 在每轮微批执行前调用，预分配线程池资源、内存缓冲区等。
     * 资源不足时抛出ResourceExhaustedException。
     */
    void allocateResources();

    /**
     * 释放指定任务占用的资源。
     * 在任务完成、失败或被终止后调用。
     *
     * @param taskId 任务唯一标识
     */
    void releaseResourcesForTask(String taskId);

    /**
     * 定期监控各任务的资源使用情况。
     * 检查各任务的CPU时间、内存占用是否超过阈值。
     * 超限任务将触发告警或终止。
     */
    void monitorTaskResources();

    /**
     * 定期监控系统整体资源使用情况。
     * 检查系统级CPU、内存、磁盘I/O和缓存使用率。
     * 超过预警阈值时触发保护措施：降低并行度、加速缓存淘汰、暂停低优先级任务。
     */
    void monitorSystemResources();

    /**
     * 强制终止指定任务。
     * 当检测到任务执行超时或资源占用异常时调用。
     * 终止后释放该任务占用的所有资源，并记录异常日志。
     *
     * @param taskId 任务唯一标识
     * @return 终止操作是否成功
     */
    boolean terminateTask(String taskId);
}
