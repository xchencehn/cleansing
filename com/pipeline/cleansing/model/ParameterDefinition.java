package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 算子参数定义
 */
public class ParameterDefinition implements Serializable {
    private String name;
    private String description;
    /** 参数类型：NUMBER, STRING, ENUM */
    private String type;
    private boolean required;
    private Object defaultValue;
    /** 数值型参数的取值范围下限 */
    private Double minValue;
    /** 数值型参数的取值范围上限 */
    private Double maxValue;
    /** 枚举型参数的可选值列表 */
    private List<String> enumValues;

    public ParameterDefinition() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public boolean isRequired() { return required; }
    public void setRequired(boolean required) { this.required = required; }
    public Object getDefaultValue() { return defaultValue; }
    public void setDefaultValue(Object defaultValue) { this.defaultValue = defaultValue; }
    public Double getMinValue() { return minValue; }
    public void setMinValue(Double minValue) { this.minValue = minValue; }
    public Double getMaxValue() { return maxValue; }
    public void setMaxValue(Double maxValue) { this.maxValue = maxValue; }
    public List<String> getEnumValues() { return enumValues; }
    public void setEnumValues(List<String> enumValues) { this.enumValues = enumValues; }
}
