package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 单个时序数据点：值 + 质量码 + 时间戳
 */
public class DataPoint implements Serializable {
    private Timestamp timestamp;
    private Object value;
    private int qualityCode;

    public DataPoint() {}

    public DataPoint(Timestamp timestamp, Object value, int qualityCode) {
        this.timestamp = timestamp;
        this.value = value;
        this.qualityCode = qualityCode;
    }

    public Timestamp getTimestamp() { return timestamp; }
    public void setTimestamp(Timestamp timestamp) { this.timestamp = timestamp; }
    public Object getValue() { return value; }
    public void setValue(Object value) { this.value = value; }
    public int getQualityCode() { return qualityCode; }
    public void setQualityCode(int qualityCode) { this.qualityCode = qualityCode; }
}
