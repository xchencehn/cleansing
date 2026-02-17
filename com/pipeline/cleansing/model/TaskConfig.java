package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.util.List;

/**
 * 清洗任务配置，定义一个测点的完整清洗管道
 */
public class TaskConfig implements Serializable {
    /** 输入测点标识 */
    private String inputPointId;
    /** 输出测点标识（清洗结果写入的目标测点） */
    private String outputPointId;
    /** 算子管道配置，按order排序执行 */
    private List<OperatorConfig> operatorPipeline;
    /** 调度优先级，数值越小优先级越高 */
    private int priority;
    /** 任务描述 */
    private String description;
    /** 计算所需的历史数据回看时间窗口（秒） */
    private int lookbackSeconds;

    public TaskConfig() {}

    public String getInputPointId() { return inputPointId; }
    public void setInputPointId(String inputPointId) { this.inputPointId = inputPointId; }
    public String getOutputPointId() { return outputPointId; }
    public void setOutputPointId(String outputPointId) { this.outputPointId = outputPointId; }
    public List<OperatorConfig> getOperatorPipeline() { return operatorPipeline; }
    public void setOperatorPipeline(List<OperatorConfig> operatorPipeline) { this.operatorPipeline = operatorPipeline; }
    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public int getLookbackSeconds() { return lookbackSeconds; }
    public void setLookbackSeconds(int lookbackSeconds) { this.lookbackSeconds = lookbackSeconds; }
}
