package com.pipeline.cleansing.core;

import com.pipeline.cleansing.model.PointData;
import com.pipeline.cleansing.model.TimeRange;
import com.pipeline.cleansing.model.TimeSeriesData;

/**
 * 数据存储器接口 —— 时序数据的持久化层。
 *
 * 提供测点元数据管理和时序数据的写入、查询能力。
 * 对上层暴露统一抽象，底层默认由SQLite引擎实现，
 * 通过切换适配层可对接GaussDB等企业级数据库。
 *
 * 底层实现要点：
 * - 一点一表的分表设计，消除写入锁竞争
 * - 时间戳B-tree索引，保证范围查询效率
 * - 按时间窗口分库，支持过期数据整体归档或删除
 * - 逢变才存压缩逻辑内置于putDataPoint实现中
 */
public interface DataStorage {

    /**
     * 新增一个测点。
     * 创建对应的存储表结构和索引。
     *
     * @param pointData 测点元数据
     * @return 新增测点的唯一标识（tag）
     */
    String addDataPoint(PointData pointData);

    /**
     * 删除指定测点。
     * 同时清理该测点的所有历史数据和存储表。
     *
     * @param tag 测点唯一标识
     * @return 操作是否成功
     */
    boolean deleteDataPoint(String tag);

    /**
     * 修改测点元数据。
     * 仅更新元数据信息，不影响已存储的历史数据。
     *
     * @param dataPointId  测点唯一标识
     * @param newPointData 新的测点元数据
     * @return 操作是否成功
     */
    boolean updateDataPoint(String dataPointId, PointData newPointData);

    /**
     * 查询指定测点的元数据。
     *
     * @param dataPointId 测点唯一标识
     * @return 测点元数据；未找到返回null
     */
    PointData queryDataPoint(String dataPointId);

    /**
     * 查询指定测点在给定时间范围内的历史时序数据。
     *
     * @param dataPointId 测点唯一标识
     * @param timeRange   查询时间范围
     * @return 时序数据序列，按时间戳升序排列
     */
    TimeSeriesData queryTimeSeriesData(String dataPointId, TimeRange timeRange);

    /**
     * 实时数据写入接口。
     * 内部实现逢变才存逻辑：与上次存储值比较，
     * 仅当差值超过死区或质量码变化或超过最大静默间隔时才实际写入。
     * 支持高性能数据流入，供采集模块和缓存写回调用。
     *
     * @param pointData 待写入的测点数据（包含tag标识和数据点）
     * @return 是否实际写入（未触发存储条件时返回false，非错误）
     */
    boolean putDataPoint(PointData pointData);
}
