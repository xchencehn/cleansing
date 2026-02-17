package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 算子管道中单个算子的配置
 */
public class OperatorConfig implements Serializable {
    /** 算子唯一标识 */
    private String functionId;
    /** 算子在管道中的执行顺序（从小到大） */
    private int order;
    /** 算子参数 */
    private Map<String, Object> parameters;

    public OperatorConfig() {}

    public OperatorConfig(String functionId, int order, Map<String, Object> parameters) {
        this.functionId = functionId;
        this.order = order;
        this.parameters = parameters;
    }

    public String getFunctionId() { return functionId; }
    public void setFunctionId(String functionId) { this.functionId = functionId; }
    public int getOrder() { return order; }
    public void setOrder(int order) { this.order = order; }
    public Map<String, Object> getParameters() { return parameters; }
    public void setParameters(Map<String, Object> parameters) { this.parameters = parameters; }
}
