package com.pipeline.cleansing.core;

/**
 * 运行时环境接口 —— 系统的骨架和生命周期管理者。
 *
 * 采用构建者模式支持链式配置，将各个管理器组件注入后统一启动。
 * Environment控制各组件的初始化顺序和依赖关系，
 * 确保系统以正确的状态进入运行。
 *
 * 典型用法：
 * <pre>
 * Environment.initialize()
 *     .setFunctionManager(functionManager)
 *     .setTaskManager(taskManager)
 *     .setResourceManager(resourceManager)
 *     .setDataStorage(dataStorage)
 *     .setTaskExecutor(taskExecutor)
 *     .start();
 * </pre>
 */
public interface Environment {

    /**
     * 初始化运行时环境。
     * 创建环境实例并准备接受组件注入。
     *
     * @return 环境实例，支持链式调用
     */
    static Environment initialize() {
        // 由实现类提供，此处仅定义接口契约
        throw new UnsupportedOperationException(
            "Use concrete implementation, e.g. DefaultEnvironment.initialize()");
    }

    /**
     * 配置算子管理器实例。
     *
     * @param functionManager 算子管理器
     * @return 当前环境实例，支持链式调用
     */
    Environment setFunctionManager(FunctionManager functionManager);

    /**
     * 配置任务管理器实例。
     *
     * @param taskManager 任务管理器
     * @return 当前环境实例，支持链式调用
     */
    Environment setTaskManager(TaskManager taskManager);

    /**
     * 配置资源管理器实例。
     *
     * @param resourceManager 资源管理器
     * @return 当前环境实例，支持链式调用
     */
    Environment setResourceManager(ResourceManager resourceManager);

    /**
     * 配置数据存储器实例。
     *
     * @param dataStorage 数据存储器
     * @return 当前环境实例，支持链式调用
     */
    Environment setDataStorage(DataStorage dataStorage);

    /**
     * 配置任务执行器实例。
     *
     * @param taskExecutor 任务执行器
     * @return 当前环境实例，支持链式调用
     */
    Environment setTaskExecutor(TaskExecutor taskExecutor);

    /**
     * 启动运行时环境。
     * 按依赖顺序初始化各组件：
     *   DataStorage → ResourceManager → FunctionManager → TaskManager → TaskExecutor
     * 所有组件启动成功后系统进入运行状态，开始微批调度循环。
     *
     * @throws IllegalStateException 必要组件未配置时抛出
     */
    void start();

    /**
     * 优雅关闭运行时环境。
     * 停止微批调度，等待当前执行中的任务完成，
     * 将缓存中的数据刷写到持久化存储，
     * 按与启动相反的顺序关闭各组件。
     */
    void shutdown();
}
