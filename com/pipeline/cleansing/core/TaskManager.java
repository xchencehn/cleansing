package com.pipeline.cleansing.core;

import com.pipeline.cleansing.model.Task;
import com.pipeline.cleansing.model.TaskConfig;
import com.pipeline.cleansing.model.TaskStatus;

import java.util.List;

/**
 * 任务管理器接口 —— 清洗任务的创建、状态跟踪和生命周期管理中枢。
 *
 * 每个任务对应一个测点的完整清洗管道定义。
 * 任务管理器维护所有任务的运行状态，为任务执行器提供待执行任务列表。
 */
public interface TaskManager {

    /**
     * 注册一个清洗任务。
     * 注册时会校验taskId唯一性和TaskConfig中引用的算子是否存在。
     *
     * @param taskId 任务唯一标识
     * @param config 任务配置
     * @throws IllegalArgumentException taskId重复或配置不合法时抛出
     */
    void registerTask(String taskId, TaskConfig config);

    /**
     * 卸载指定任务。
     * 如果任务当前正在执行，会先终止执行再卸载。
     *
     * @param taskId 任务唯一标识
     */
    void unregisterTask(String taskId);

    /**
     * 获取指定任务。
     *
     * @param taskId 任务唯一标识
     * @return 任务实体；未找到返回null
     */
    Task getTask(String taskId);

    /**
     * 批量获取任务。
     *
     * @param taskIds 任务标识列表
     * @return 任务实体列表；不存在的标识对应位置为null
     */
    List<Task> getTasks(List<String> taskIds);

    /**
     * 获取所有已注册任务。
     *
     * @return 全部任务列表
     */
    List<Task> getAllTasks();

    /**
     * 获取当前所有处于活跃状态（READY）的待执行任务。
     * 在每个微批周期被任务执行器调用。
     * 返回结果按任务优先级排序。
     *
     * @return 活跃任务列表，按优先级升序排列
     */
    List<Task> getActiveTasks();

    /**
     * 更新指定任务的运行状态。
     *
     * @param taskId 任务唯一标识
     * @param status 目标状态
     * @return 更新是否成功（任务不存在或状态转换不合法时返回false）
     */
    boolean updateTaskStatus(String taskId, TaskStatus status);
}
