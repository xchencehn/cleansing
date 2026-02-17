package com.pipeline.cleansing.operators;

import com.pipeline.cleansing.core.OperatorContext;
import com.pipeline.cleansing.core.UFunction;
import com.pipeline.cleansing.model.*;

import java.util.Arrays;
import java.util.List;

/**
 * 阈值检测算子。
 * 基于预设的上下限范围检测异常值，超出范围的数据点按配置策略替换。
 *
 * 参数：
 * - upperLimit: 数值上限 (NUMBER, 必选)
 * - lowerLimit: 数值下限 (NUMBER, 必选)
 * - replaceStrategy: 替换策略 (ENUM: FIXED_VALUE / LAST_VALID / WINDOW_MEAN, 默认 LAST_VALID)
 * - fixedValue: 固定替换值 (NUMBER, 当策略为FIXED_VALUE时必选)
 * - windowSize: 滑动窗口大小 (NUMBER, 当策略为WINDOW_MEAN时使用, 默认10)
 */
public class ThresholdDetectionOperator implements UFunction {

    private double upperLimit;
    private double lowerLimit;
    private String replaceStrategy;
    private double fixedValue;
    private int windowSize;

    @Override
    public void initialize(OperatorContext context) {
        this.upperLimit = context.getParameter("upperLimit", Double.MAX_VALUE);
        this.lowerLimit = context.getParameter("lowerLimit", -Double.MAX_VALUE);
        this.replaceStrategy = context.getParameter("replaceStrategy", "LAST_VALID");
        this.fixedValue = context.getParameter("fixedValue", 0.0);
        this.windowSize = context.getParameter("windowSize", 10);
    }

    @Override
    public void execute(OperatorContext context) {
        TimeSeriesData input = context.getInputData(null);
        if (input == null || input.isEmpty()) return;

        List<DataPoint> points = input.getDataPoints();
        DataPoint lastValid = null;

        for (int i = 0; i < points.size(); i++) {
            DataPoint dp = points.get(i);
            double value = ((Number) dp.getValue()).doubleValue();

            if (value < lowerLimit || value > upperLimit) {
                // 异常值，执行替换
                double replacedValue = computeReplacement(points, i, lastValid);
                // 质量码标记为已修正（192 = 修正值标记）
                context.setOutputValue(dp.getTimestamp(), replacedValue, 192);
            } else {
                // 正常值，直接输出
                lastValid = dp;
                context.setOutputValue(dp.getTimestamp(), value, dp.getQualityCode());
            }
        }
    }

    private double computeReplacement(List<DataPoint> points, int index, DataPoint lastValid) {
        switch (replaceStrategy) {
            case "FIXED_VALUE":
                return fixedValue;

            case "LAST_VALID":
                return (lastValid != null)
                        ? ((Number) lastValid.getValue()).doubleValue()
                        : fixedValue; // 没有历史有效值时降级为固定值

            case "WINDOW_MEAN":
                return computeWindowMean(points, index);

            default:
                return (lastValid != null)
                        ? ((Number) lastValid.getValue()).doubleValue()
                        : fixedValue;
        }
    }

    private double computeWindowMean(List<DataPoint> points, int currentIndex) {
        double sum = 0;
        int count = 0;
        int start = Math.max(0, currentIndex - windowSize);

        for (int i = start; i < currentIndex; i++) {
            double val = ((Number) points.get(i).getValue()).doubleValue();
            if (val >= lowerLimit && val <= upperLimit) {
                sum += val;
                count++;
            }
        }

        return (count > 0) ? (sum / count) : fixedValue;
    }

    @Override
    public void cleanup() {
        // 无需清理
    }

    @Override
    public FunctionMetadata getMetadata() {
        FunctionMetadata meta = new FunctionMetadata();
        meta.setFunctionId("threshold_detection");
        meta.setName("阈值检测算子");
        meta.setVersion("1.0.0");
        meta.setDescription("基于预设上下限范围检测异常值，对管道压力突发尖峰、传感器重启无效零值等脉冲型异常效果显著。");
        meta.setApplicableTypes(Arrays.asList("PRESSURE", "TEMPERATURE", "FLOW", "VIBRATION"));

        ParameterDefinition upperDef = new ParameterDefinition();
        upperDef.setName("upperLimit");
        upperDef.setType("NUMBER");
        upperDef.setRequired(true);
        upperDef.setDescription("数值上限，超过此值判定为异常");

        ParameterDefinition lowerDef = new ParameterDefinition();
        lowerDef.setName("lowerLimit");
        lowerDef.setType("NUMBER");
        lowerDef.setRequired(true);
        lowerDef.setDescription("数值下限，低于此值判定为异常");

        ParameterDefinition strategyDef = new ParameterDefinition();
        strategyDef.setName("replaceStrategy");
        strategyDef.setType("ENUM");
        strategyDef.setRequired(false);
        strategyDef.setDefaultValue("LAST_VALID");
        strategyDef.setEnumValues(Arrays.asList("FIXED_VALUE", "LAST_VALID", "WINDOW_MEAN"));
        strategyDef.setDescription("异常值替换策略");

        ParameterDefinition fixedDef = new ParameterDefinition();
        fixedDef.setName("fixedValue");
        fixedDef.setType("NUMBER");
        fixedDef.setRequired(false);
        fixedDef.setDefaultValue(0.0);
        fixedDef.setDescription("固定替换值（策略为FIXED_VALUE时使用）");

        ParameterDefinition windowDef = new ParameterDefinition();
        windowDef.setName("windowSize");
        windowDef.setType("NUMBER");
        windowDef.setRequired(false);
        windowDef.setDefaultValue(10.0);
        windowDef.setMinValue(2.0);
        windowDef.setMaxValue(1000.0);
        windowDef.setDescription("滑动窗口大小（策略为WINDOW_MEAN时使用）");

        meta.setParameterDefinitions(Arrays.asList(upperDef, lowerDef, strategyDef, fixedDef, windowDef));
        return meta;
    }
}
