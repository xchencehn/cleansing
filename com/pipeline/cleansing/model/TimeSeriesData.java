package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 时序数据序列，包含一个测点在某时间范围内的有序数据点集合
 */
public class TimeSeriesData implements Serializable {
    private String pointId;
    private List<DataPoint> dataPoints;

    public TimeSeriesData() {
        this.dataPoints = new ArrayList<>();
    }

    public TimeSeriesData(String pointId) {
        this.pointId = pointId;
        this.dataPoints = new ArrayList<>();
    }

    public String getPointId() { return pointId; }
    public void setPointId(String pointId) { this.pointId = pointId; }

    public List<DataPoint> getDataPoints() {
        return Collections.unmodifiableList(dataPoints);
    }

    public void addDataPoint(DataPoint point) {
        this.dataPoints.add(point);
    }

    public int size() {
        return dataPoints.size();
    }

    public boolean isEmpty() {
        return dataPoints.isEmpty();
    }
}
