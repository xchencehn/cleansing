package com.pipeline.cleansing.core;

import com.pipeline.cleansing.model.TimeRange;
import com.pipeline.cleansing.model.TimeSeriesData;

import java.sql.Timestamp;

/**
 * 算子上下文接口 —— 算子与系统交互的唯一桥梁。
 *
 * 为算子提供四个核心能力：
 * 1. 获取输入数据
 * 2. 输出清洗结果
 * 3. 读取配置参数
 * 4. 查询历史数据（支持跨测点）
 *
 * 算子通过此接口获取所需的一切信息，无需感知底层的缓存机制、
 * 存储引擎或数据分发逻辑。
 */
public interface OperatorContext {

    /**
     * 获取指定时间范围内的输入时序数据。
     * 数据可能来自内存缓存或持久化存储，算子无需感知数据来源。
     *
     * @param timeRange 时间范围；传入null则返回当前微批周期内的全部可用数据
     * @return 输入时序数据序列，按时间戳升序排列；无数据时返回空序列（非null）
     */
    TimeSeriesData getInputData(TimeRange timeRange);

    /**
     * 向输出数据流写入一个带时间戳和质量码的数据点。
     * 输出的数据点将进入缓存管理，后续可能被下游算子消费、
     * 异步持久化到存储层或推送至数据分发模块。
     *
     * @param timestamp   数据点时间戳
     * @param value       数据值
     * @param qualityCode 质量码
     * @return 写入是否成功
     */
    boolean setOutputValue(Timestamp timestamp, Object value, int qualityCode);

    /**
     * 获取指定名称的算子参数，支持泛型类型安全转换。
     * 参数在任务配置时设定，运行时通过此方法读取。
     *
     * @param paramName    参数名称
     * @param defaultValue 参数不存在时的默认值，同时用于推断返回类型
     * @param <T>          参数值类型
     * @return 参数值；参数不存在时返回defaultValue
     */
    <T> T getParameter(String paramName, T defaultValue);

    /**
     * 查询指定测点在特定时间范围内的历史数据。
     * 支持跨测点数据访问，使算子能够利用相邻测点的数据
     * 进行交叉验证或辅助判断。
     *
     * @param pointId   目标测点标识
     * @param timeRange 查询时间范围
     * @return 历史时序数据序列；无数据时返回空序列（非null）
     */
    TimeSeriesData queryHistoricalData(String pointId, TimeRange timeRange);
}
