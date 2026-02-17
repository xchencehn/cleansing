package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.util.Map;

/**
 * 测点元数据，包含测点的物理属性和存储参数
 */
public class PointData implements Serializable {
    /** 测点唯一标识（tag名） */
    private String tag;
    /** 测点显示名称 */
    private String name;
    /** 物理单位 */
    private String unit;
    /** 量程下限 */
    private double rangeLow;
    /** 量程上限 */
    private double rangeHigh;
    /** 所属管段标识 */
    private String pipelineSegment;
    /** 逢变才存死区参数 */
    private double deadband;
    /** 最大静默间隔（秒） */
    private int maxSilentInterval;
    /** 数据保留周期（天） */
    private int retentionDays;
    /** 扩展属性 */
    private Map<String, Object> attributes;

    public PointData() {}

    public String getTag() { return tag; }
    public void setTag(String tag) { this.tag = tag; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getUnit() { return unit; }
    public void setUnit(String unit) { this.unit = unit; }
    public double getRangeLow() { return rangeLow; }
    public void setRangeLow(double rangeLow) { this.rangeLow = rangeLow; }
    public double getRangeHigh() { return rangeHigh; }
    public void setRangeHigh(double rangeHigh) { this.rangeHigh = rangeHigh; }
    public String getPipelineSegment() { return pipelineSegment; }
    public void setPipelineSegment(String pipelineSegment) { this.pipelineSegment = pipelineSegment; }
    public double getDeadband() { return deadband; }
    public void setDeadband(double deadband) { this.deadband = deadband; }
    public int getMaxSilentInterval() { return maxSilentInterval; }
    public void setMaxSilentInterval(int maxSilentInterval) { this.maxSilentInterval = maxSilentInterval; }
    public int getRetentionDays() { return retentionDays; }
    public void setRetentionDays(int retentionDays) { this.retentionDays = retentionDays; }
    public Map<String, Object> getAttributes() { return attributes; }
    public void setAttributes(Map<String, Object> attributes) { this.attributes = attributes; }
}
