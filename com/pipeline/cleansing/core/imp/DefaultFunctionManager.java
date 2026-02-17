package com.pipeline.cleansing.core.impl;

import com.pipeline.cleansing.core.FunctionManager;
import com.pipeline.cleansing.core.UFunction;
import com.pipeline.cleansing.model.FunctionMetadata;
import com.pipeline.cleansing.model.ParameterDefinition;
import com.pipeline.cleansing.model.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 算子管理器默认实现。
 * 使用ConcurrentHashMap存储算子注册表，支持并发注册和查询。
 */
public class DefaultFunctionManager implements FunctionManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultFunctionManager.class);

    /** 算子注册表：functionId -> UFunction实例 */
    private final ConcurrentHashMap<String, UFunction> functionRegistry = new ConcurrentHashMap<>();

    @Override
    public boolean registerFunction(String functionId, UFunction uFunction) {
        if (functionId == null || functionId.isBlank()) {
            log.error("Cannot register function with null or blank id");
            return false;
        }
        if (uFunction == null) {
            log.error("Cannot register null function for id: {}", functionId);
            return false;
        }

        UFunction existing = functionRegistry.putIfAbsent(functionId, uFunction);
        if (existing != null) {
            log.warn("Function '{}' is already registered, registration rejected.", functionId);
            return false;
        }

        log.info("Function '{}' registered successfully. Version: {}",
                functionId, uFunction.getMetadata().getVersion());
        return true;
    }

    @Override
    public boolean unregisterFunction(String functionId) {
        if (functionId == null || functionId.isBlank()) {
            return false;
        }

        UFunction removed = functionRegistry.remove(functionId);
        if (removed == null) {
            log.warn("Function '{}' not found, nothing to unregister.", functionId);
            return false;
        }

        try {
            removed.cleanup();
        } catch (Exception e) {
            log.error("Error during cleanup of function '{}': {}", functionId, e.getMessage(), e);
        }

        log.info("Function '{}' unregistered successfully.", functionId);
        return true;
    }

    @Override
    public UFunction getFunction(String functionId) {
        return functionRegistry.get(functionId);
    }

    @Override
    public Map<String, UFunction> getFunctions(List<String> functionIds) {
        if (functionIds == null || functionIds.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, UFunction> result = new HashMap<>();
        for (String id : functionIds) {
            UFunction fn = functionRegistry.get(id);
            if (fn != null) {
                result.put(id, fn);
            }
        }
        return result;
    }

    @Override
    public List<UFunction> getAllFunctions() {
        return new ArrayList<>(functionRegistry.values());
    }

    @Override
    public ValidationResult validateFunction(String functionId, Map<String, Object> parameters) {
        ValidationResult result = new ValidationResult();

        // 检查算子是否存在
        UFunction function = functionRegistry.get(functionId);
        if (function == null) {
            result.addError("Function '" + functionId + "' is not registered.");
            return result;
        }

        FunctionMetadata metadata = function.getMetadata();
        if (metadata == null || metadata.getParameterDefinitions() == null) {
            // 算子无参数定义，跳过参数校验
            return result;
        }

        Map<String, Object> params = (parameters != null) ? parameters : Collections.emptyMap();
        List<ParameterDefinition> definitions = metadata.getParameterDefinitions();

        for (ParameterDefinition def : definitions) {
            String paramName = def.getName();
            Object value = params.get(paramName);

            // 必选参数缺失检查
            if (def.isRequired() && value == null) {
                result.addError("Required parameter '" + paramName + "' is missing.");
                continue;
            }

            if (value == null) {
                continue; // 可选参数未提供，使用默认值
            }

            // 类型检查与范围校验
            switch (def.getType().toUpperCase()) {
                case "NUMBER":
                    if (!(value instanceof Number)) {
                        result.addError("Parameter '" + paramName
                                + "' expects NUMBER type, got: " + value.getClass().getSimpleName());
                    } else {
                        double numVal = ((Number) value).doubleValue();
                        if (def.getMinValue() != null && numVal < def.getMinValue()) {
                            result.addError("Parameter '" + paramName + "' value "
                                    + numVal + " is below minimum " + def.getMinValue());
                        }
                        if (def.getMaxValue() != null && numVal > def.getMaxValue()) {
                            result.addError("Parameter '" + paramName + "' value "
                                    + numVal + " exceeds maximum " + def.getMaxValue());
                        }
                    }
                    break;

                case "STRING":
                    if (!(value instanceof String)) {
                        result.addError("Parameter '" + paramName
                                + "' expects STRING type, got: " + value.getClass().getSimpleName());
                    }
                    break;

                case "ENUM":
                    if (!(value instanceof String)) {
                        result.addError("Parameter '" + paramName
                                + "' expects ENUM (String) type, got: " + value.getClass().getSimpleName());
                    } else if (def.getEnumValues() != null
                            && !def.getEnumValues().contains((String) value)) {
                        result.addError("Parameter '" + paramName + "' value '"
                                + value + "' is not in allowed values: " + def.getEnumValues());
                    }
                    break;

                default:
                    result.addWarning("Unknown parameter type '" + def.getType()
                            + "' for parameter '" + paramName + "', skipping validation.");
            }
        }

        // 检查是否有多余的未定义参数
        Set<String> definedNames = definitions.stream()
                .map(ParameterDefinition::getName)
                .collect(Collectors.toSet());
        for (String key : params.keySet()) {
            if (!definedNames.contains(key)) {
                result.addWarning("Parameter '" + key + "' is not defined in function '"
                        + functionId + "' metadata, it will be ignored.");
            }
        }

        return result;
    }
}
