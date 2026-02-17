package com.pipeline.cleansing.core;

import com.pipeline.cleansing.model.Task;

/**
 * 任务执行器接口 —— 清洗任务的实际执行引擎。
 *
 * 按微批周期从任务管理器拉取任务，执行前置检查，
 * 驱动算子管道执行清洗计算，执行后收集统计信息并处理异常。
 *
 * 执行流程：
 *   checkTaskStatus → checkTaskInputData → checkOperatorParameters → executeTask
 *   （monitorExecution 在执行过程中持续运行）
 */
public interface TaskExecutor {

    /**
     * 检查系统当前是否满足任务执行条件。
     * 包括：系统运行状态、资源可用性、上一轮任务是否完成等。
     * 不满足条件时记录日志并跳过本轮执行。
     */
    void checkTaskStatus();

    /**
     * 检查指定任务的输入数据是否合规。
     * 校验内容包括：
     * - 输入数据是否存在（缓存或存储中有该测点的数据）
     * - 数据时间范围是否满足任务要求
     * - 数据质量预评估（离群值比例、缺失率等）
     *
     * @param task 待检查的任务
     * @return 输入数据是否合规
     */
    boolean checkTaskInputData(Task task);

    /**
     * 检查任务中各算子的参数是否符合预期。
     * 调用FunctionManager.validateFunction逐一验证管道中各算子的参数。
     *
     * @param task 待检查的任务
     * @return 所有算子参数是否合规
     */
    boolean checkOperatorParameters(Task task);

    /**
     * 执行指定的清洗任务。
     * 按算子管道定义的顺序依次调用各算子的execute方法，
     * 前一个算子的输出作为后一个算子的输入。
     * 执行过程中捕获异常，区分可恢复异常和致命异常分别处理。
     *
     * @param task 待执行的任务
     */
    void executeTask(Task task);

    /**
     * 实时监控任务执行状态。
     * 与资源管理器协同工作，持续检测：
     * - 任务执行是否超时
     * - 任务资源占用是否异常
     * - 是否需要触发任务终止
     * 在每轮微批执行期间以独立线程持续运行。
     */
    void monitorExecution();
}
