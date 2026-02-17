package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 清洗任务实体，封装任务配置与运行时状态
 */
public class Task implements Serializable {
    private String taskId;
    private TaskConfig config;
    private TaskStatus status;
    private Timestamp createTime;
    private Timestamp lastExecuteTime;
    private long executionCount;
    private long failureCount;
    private String lastErrorMessage;

    public Task() {}

    public Task(String taskId, TaskConfig config) {
        this.taskId = taskId;
        this.config = config;
        this.status = TaskStatus.CREATED;
        this.createTime = new Timestamp(System.currentTimeMillis());
        this.executionCount = 0;
        this.failureCount = 0;
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public TaskConfig getConfig() { return config; }
    public void setConfig(TaskConfig config) { this.config = config; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
    public Timestamp getCreateTime() { return createTime; }
    public void setCreateTime(Timestamp createTime) { this.createTime = createTime; }
    public Timestamp getLastExecuteTime() { return lastExecuteTime; }
    public void setLastExecuteTime(Timestamp lastExecuteTime) { this.lastExecuteTime = lastExecuteTime; }
    public long getExecutionCount() { return executionCount; }
    public void setExecutionCount(long executionCount) { this.executionCount = executionCount; }
    public long getFailureCount() { return failureCount; }
    public void setFailureCount(long failureCount) { this.failureCount = failureCount; }
    public String getLastErrorMessage() { return lastErrorMessage; }
    public void setLastErrorMessage(String lastErrorMessage) { this.lastErrorMessage = lastErrorMessage; }
}
