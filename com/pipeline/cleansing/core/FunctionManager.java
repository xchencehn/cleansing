package com.pipeline.cleansing.core;

import com.pipeline.cleansing.model.ValidationResult;

import java.util.List;
import java.util.Map;

/**
 * 算子管理器接口 —— 系统的算子注册表。
 *
 * 负责算子的注册、卸载、查询和参数验证。
 * 算子通过反射机制动态加载，支持运行时热更新。
 */
public interface FunctionManager {

    /**
     * 注册一个算子。
     * 注册时会检查functionId唯一性，重复注册将返回false。
     *
     * @param functionId 算子唯一标识
     * @param uFunction  算子实例
     * @return 注册是否成功
     */
    boolean registerFunction(String functionId, UFunction uFunction);

    /**
     * 卸载指定算子。
     * 卸载前会检查是否有活跃任务依赖该算子，若有则拒绝卸载并返回false。
     *
     * @param functionId 算子唯一标识
     * @return 卸载是否成功
     */
    boolean unregisterFunction(String functionId);

    /**
     * 获取指定算子实例。
     *
     * @param functionId 算子唯一标识
     * @return 算子实例；未找到返回null
     */
    UFunction getFunction(String functionId);

    /**
     * 批量获取多个算子。
     *
     * @param functionIds 算子标识列表
     * @return 标识到算子实例的映射；不存在的标识不包含在结果中
     */
    Map<String, UFunction> getFunctions(List<String> functionIds);

    /**
     * 获取所有已注册的算子。
     *
     * @return 全部算子实例列表
     */
    List<UFunction> getAllFunctions();

    /**
     * 验证指定算子是否可用及其参数是否合规。
     * 在任务配置阶段调用，将配置错误拦截在任务启动之前。
     *
     * 校验内容包括：
     * - 算子是否已注册
     * - 必选参数是否缺失
     * - 参数类型是否匹配
     * - 数值参数是否在合法范围内
     * - 枚举参数是否为合法选项
     *
     * @param functionId 算子唯一标识
     * @param parameters 待验证的参数集合
     * @return 详细的验证结果
     */
    ValidationResult validateFunction(String functionId, Map<String, Object> parameters);
}
