package com.pipeline.cleansing.core.impl;

import com.pipeline.cleansing.core.TaskManager;
import com.pipeline.cleansing.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 任务管理器默认实现。
 * 维护所有任务的注册表和状态，提供任务生命周期管理。
 */
public class DefaultTaskManager implements TaskManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskManager.class);

    /** 任务注册表：taskId -> Task */
    private final ConcurrentHashMap<String, Task> taskRegistry = new ConcurrentHashMap<>();

    /** 合法的状态转换表 */
    private static final Map<TaskStatus, Set<TaskStatus>> VALID_TRANSITIONS = new HashMap<>();

    static {
        VALID_TRANSITIONS.put(TaskStatus.CREATED,
                EnumSet.of(TaskStatus.READY, TaskStatus.TERMINATED));
        VALID_TRANSITIONS.put(TaskStatus.READY,
                EnumSet.of(TaskStatus.RUNNING, TaskStatus.PAUSED, TaskStatus.TERMINATED));
        VALID_TRANSITIONS.put(TaskStatus.RUNNING,
                EnumSet.of(TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TERMINATED));
        VALID_TRANSITIONS.put(TaskStatus.COMPLETED,
                EnumSet.of(TaskStatus.READY, TaskStatus.TERMINATED));
        VALID_TRANSITIONS.put(TaskStatus.FAILED,
                EnumSet.of(TaskStatus.READY, TaskStatus.FAULTED, TaskStatus.TERMINATED));
        VALID_TRANSITIONS.put(TaskStatus.PAUSED,
                EnumSet.of(TaskStatus.READY, TaskStatus.TERMINATED));
        VALID_TRANSITIONS.put(TaskStatus.FAULTED,
                EnumSet.of(TaskStatus.READY, TaskStatus.TERMINATED));
        VALID_TRANSITIONS.put(TaskStatus.TERMINATED,
                Collections.emptySet());
    }

    @Override
    public void registerTask(String taskId, TaskConfig config) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("Task ID must not be null or blank");
        }
        if (config == null) {
            throw new IllegalArgumentException("TaskConfig must not be null for task: " + taskId);
        }
        if (config.getInputPointId() == null || config.getInputPointId().isBlank()) {
            throw new IllegalArgumentException("Input point ID is required for task: " + taskId);
        }
        if (config.getOperatorPipeline() == null || config.getOperatorPipeline().isEmpty()) {
            throw new IllegalArgumentException("Operator pipeline must not be empty for task: " + taskId);
        }

        if (taskRegistry.containsKey(taskId)) {
            throw new IllegalArgumentException("Task '" + taskId + "' is already registered");
        }

        Task task = new Task(taskId, config);
        task.setStatus(TaskStatus.READY); // 注册后直接进入就绪状态
        taskRegistry.put(taskId, task);

        log.info("Task '{}' registered. Input: {}, Pipeline size: {}, Priority: {}",
                taskId, config.getInputPointId(),
                config.getOperatorPipeline().size(), config.getPriority());
    }

    @Override
    public void unregisterTask(String taskId) {
        Task task = taskRegistry.remove(taskId);
        if (task == null) {
            log.warn("Task '{}' not found, nothing to unregister.", taskId);
            return;
        }
        if (task.getStatus() == TaskStatus.RUNNING) {
            log.warn("Task '{}' is currently running, marking as terminated.", taskId);
            task.setStatus(TaskStatus.TERMINATED);
        }
        log.info("Task '{}' unregistered.", taskId);
    }

    @Override
    public Task getTask(String taskId) {
        return taskRegistry.get(taskId);
    }

    @Override
    public List<Task> getTasks(List<String> taskIds) {
        if (taskIds == null) return Collections.emptyList();
        List<Task> result = new ArrayList<>(taskIds.size());
        for (String id : taskIds) {
            result.add(taskRegistry.get(id)); // 不存在的返回null
        }
        return result;
    }

    @Override
    public List<Task> getAllTasks() {
        return new ArrayList<>(taskRegistry.values());
    }

    @Override
    public List<Task> getActiveTasks() {
        return taskRegistry.values().stream()
                .filter(t -> t.getStatus() == TaskStatus.READY
                        || t.getStatus() == TaskStatus.COMPLETED) // COMPLETED任务在下一轮重新变为可执行
                .sorted(Comparator.comparingInt(t -> t.getConfig().getPriority()))
                .collect(Collectors.toList());
    }

    @Override
    public boolean updateTaskStatus(String taskId, TaskStatus newStatus) {
        Task task = taskRegistry.get(taskId);
        if (task == null) {
            log.warn("Cannot update status: task '{}' not found.", taskId);
            return false;
        }

        TaskStatus currentStatus = task.getStatus();
        Set<TaskStatus> allowed = VALID_TRANSITIONS.getOrDefault(currentStatus, Collections.emptySet());

        if (!allowed.contains(newStatus)) {
            log.warn("Invalid status transition for task '{}': {} -> {}",
                    taskId, currentStatus, newStatus);
            return false;
        }

        task.setStatus(newStatus);

        if (newStatus == TaskStatus.COMPLETED) {
            task.setExecutionCount(task.getExecutionCount() + 1);
            task.setLastExecuteTime(new java.sql.Timestamp(System.currentTimeMillis()));
            // 完成后自动重置为READY，准备下一轮调度
            task.setStatus(TaskStatus.READY);
        } else if (newStatus == TaskStatus.FAILED) {
            task.setFailureCount(task.getFailureCount() + 1);
        }

        return true;
    }
}
