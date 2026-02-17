package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.util.List;

/**
 * 算子元数据，描述算子的身份、版本、参数定义和用途
 */
public class FunctionMetadata implements Serializable {
    private String functionId;
    private String name;
    private String version;
    private String description;
    /** 适用的物理量类型（如 PRESSURE, TEMPERATURE, FLOW 等） */
    private List<String> applicableTypes;
    /** 算子参数定义列表 */
    private List<ParameterDefinition> parameterDefinitions;

    public FunctionMetadata() {}

    public String getFunctionId() { return functionId; }
    public void setFunctionId(String functionId) { this.functionId = functionId; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public List<String> getApplicableTypes() { return applicableTypes; }
    public void setApplicableTypes(List<String> applicableTypes) { this.applicableTypes = applicableTypes; }
    public List<ParameterDefinition> getParameterDefinitions() { return parameterDefinitions; }
    public void setParameterDefinitions(List<ParameterDefinition> parameterDefinitions) { this.parameterDefinitions = parameterDefinitions; }
}
