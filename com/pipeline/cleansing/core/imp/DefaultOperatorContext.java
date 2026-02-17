package com.pipeline.cleansing.core.impl;

import com.pipeline.cleansing.core.DataStorage;
import com.pipeline.cleansing.core.OperatorContext;
import com.pipeline.cleansing.model.DataPoint;
import com.pipeline.cleansing.model.TimeRange;
import com.pipeline.cleansing.model.TimeSeriesData;

import java.sql.Timestamp;
import java.util.Map;

/**
 * 算子上下文默认实现。
 * 封装单个算子在管道中执行时所需的全部环境信息。
 * 支持管道串联——后一个算子的输入来自前一个算子的输出。
 */
public class DefaultOperatorContext implements OperatorContext {

    private final String inputPointId;
    private final String outputPointId;
    private final Map<String, Object> parameters;
    private final CacheManager cacheManager;
    private final DataStorage dataStorage;

    /** 前一个算子的上下文（用于管道串联），为null表示管道第一个算子 */
    private final DefaultOperatorContext previousContext;

    /** 是否为管道最后一个算子 */
    private final boolean isLastInPipeline;

    /** 当前算子的输出数据缓冲 */
    private final TimeSeriesData outputBuffer;

    public DefaultOperatorContext(String inputPointId,
                                 String outputPointId,
                                 Map<String, Object> parameters,
                                 CacheManager cacheManager,
                                 DataStorage dataStorage,
                                 DefaultOperatorContext previousContext,
                                 boolean isLastInPipeline) {
        this.inputPointId = inputPointId;
        this.outputPointId = outputPointId;
        this.parameters = parameters != null ? parameters : Map.of();
        this.cacheManager = cacheManager;
        this.dataStorage = dataStorage;
        this.previousContext = previousContext;
        this.isLastInPipeline = isLastInPipeline;
        this.outputBuffer = new TimeSeriesData(outputPointId);
    }

    @Override
    public TimeSeriesData getInputData(TimeRange timeRange) {
        // 管道串联：如果有前置算子，从前置算子的输出读取
        if (previousContext != null) {
            TimeSeriesData prevOutput = previousContext.getOutputBuffer();
            if (timeRange == null) {
                return prevOutput;
            }
            // 按时间范围过滤
            TimeSeriesData filtered = new TimeSeriesData(inputPointId);
            for (DataPoint dp : prevOutput.getDataPoints()) {
                if (!dp.getTimestamp().before(timeRange.getStart())
                        && !dp.getTimestamp().after(timeRange.getEnd())) {
                    filtered.addDataPoint(dp);
                }
            }
            return filtered;
        }

        // 管道第一个算子：从缓存读取
        TimeSeriesData cached = cacheManager.getData(inputPointId, timeRange);
        if (cached != null && !cached.isEmpty()) {
            cacheManager.resetTTL(inputPointId); // 访问时重置TTL
            return cached;
        }

        // 缓存未命中，回查存储
        if (timeRange != null) {
            TimeSeriesData stored = dataStorage.queryTimeSeriesData(inputPointId, timeRange);
            if (stored != null && !stored.isEmpty()) {
                cacheManager.loadData(inputPointId, stored);
                return stored;
            }
        }

        return new TimeSeriesData(inputPointId);
    }

    @Override
    public boolean setOutputValue(Timestamp timestamp, Object value, int qualityCode) {
        if (timestamp == null) return false;

        DataPoint point = new DataPoint(timestamp, value, qualityCode);
        outputBuffer.addDataPoint(point);

        // 如果是管道最后一个算子，将结果写入缓存
        if (isLastInPipeline) {
            cacheManager.putResult(outputPointId, point);
        }

        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getParameter(String paramName, T defaultValue) {
        Object value = parameters.get(paramName);
        if (value == null) {
            return defaultValue;
        }

        try {
            if (defaultValue != null) {
                Class<?> targetType = defaultValue.getClass();
                // 数值类型转换
                if (targetType == Double.class && value instanceof Number) {
                    return (T) Double.valueOf(((Number) value).doubleValue());
                }
                if (targetType == Integer.class && value instanceof Number) {
                    return (T) Integer.valueOf(((Number) value).intValue());
                }
                if (targetType == Long.class && value instanceof Number) {
                    return (T) Long.valueOf(((Number) value).longValue());
                }
                if (targetType == Float.class && value instanceof Number) {
                    return (T) Float.valueOf(((Number) value).floatValue());
                }
            }
            return (T) value;
        } catch (ClassCastException e) {
            return defaultValue;
        }
    }

    @Override
    public TimeSeriesData queryHistoricalData(String pointId, TimeRange timeRange) {
        // 先查缓存
        TimeSeriesData cached = cacheManager.getData(pointId, timeRange);
        if (cached != null && !cached.isEmpty()) {
            return cached;
        }
        // 缓存未命中，查存储
        TimeSeriesData stored = dataStorage.queryTimeSeriesData(pointId, timeRange);
        return (stored != null) ? stored : new TimeSeriesData(pointId);
    }

    /** 获取当前算子的输出缓冲（供下一个算子读取） */
    TimeSeriesData getOutputBuffer() {
        return outputBuffer;
    }
}
