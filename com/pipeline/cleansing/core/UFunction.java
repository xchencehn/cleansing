package com.pipeline.cleansing.core;

import com.pipeline.cleansing.model.FunctionMetadata;

/**
 * 统一算子接口 —— 所有清洗算子的基础契约。
 *
 * 算子是清洗逻辑的最小执行单元，封装一种特定的时序数据处理方法。
 * 多个算子按指定顺序串联组成算子管道，数据依次流经各算子完成清洗。
 *
 * 算子生命周期：
 *   initialize → (execute)* → cleanup
 *
 * 实现约定：
 * - 算子实现必须是线程安全的，因为同一算子实例可能被多个任务并发调用
 * - execute方法应在合理时间内完成，超时将被资源管理器强制终止
 * - 算子不应直接持有系统级资源（如数据库连接），所有交互通过OperatorContext完成
 */
public interface UFunction {

    /**
     * 算子初始化。在算子首次加载时调用一次。
     * 完成参数解析、内部状态初始化和资源预分配。
     *
     * @param context 算子上下文，提供参数读取和数据访问能力
     */
    void initialize(OperatorContext context);

    /**
     * 执行清洗逻辑。在每个微批周期被调用。
     * 通过context获取输入数据，执行处理逻辑，通过context输出结果。
     *
     * @param context 算子上下文，提供当前微批周期的数据和参数
     */
    void execute(OperatorContext context);

    /**
     * 算子清理。在算子卸载时调用。
     * 释放initialize阶段分配的资源，清理内部状态。
     */
    void cleanup();

    /**
     * 返回算子的元数据信息。
     * 包括名称、版本、参数定义和适用说明，
     * 用于管理界面展示、参数校验和算子发现。
     *
     * @return 算子元数据
     */
    FunctionMetadata getMetadata();
}
