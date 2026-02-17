package com.pipeline.cleansing.model;

/**
 * 任务状态枚举
 */
public enum TaskStatus {
    /** 已创建，尚未启动 */
    CREATED,
    /** 就绪，等待调度执行 */
    READY,
    /** 正在执行中 */
    RUNNING,
    /** 执行成功完成 */
    COMPLETED,
    /** 执行失败 */
    FAILED,
    /** 已暂停 */
    PAUSED,
    /** 已终止 */
    TERMINATED,
    /** 故障标记，自动跳过调度 */
    FAULTED
}
