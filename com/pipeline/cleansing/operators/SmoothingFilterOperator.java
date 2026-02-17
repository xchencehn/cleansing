package com.pipeline.cleansing.operators;

import com.pipeline.cleansing.core.OperatorContext;
import com.pipeline.cleansing.core.UFunction;
import com.pipeline.cleansing.model.*;

import java.util.Arrays;
import java.util.List;

/**
 * 滤波平滑算子。
 * 基于滑动窗口对时序数据进行噪声平滑。
 *
 * 参数：
 * - filterType: 滤波类型 (ENUM: SMA / EWMA / MEDIAN, 默认 SMA)
 * - windowSize: 滑动窗口大小 (NUMBER, 默认5)
 * - alpha: EWMA衰减系数 (NUMBER, 0-1, 仅EWMA模式使用, 默认0.3)
 */
public class SmoothingFilterOperator implements UFunction {

    private String filterType;
    private int windowSize;
    private double alpha;

    @Override
    public void initialize(OperatorContext context) {
        this.filterType = context.getParameter("filterType", "SMA");
        this.windowSize = context.getParameter("windowSize", 5);
        this.alpha = context.getParameter("alpha", 0.3);
    }

    @Override
    public void execute(OperatorContext context) {
        TimeSeriesData input = context.getInputData(null);
        if (input == null || input.isEmpty()) return;

        List<DataPoint> points = input.getDataPoints();

        switch (filterType) {
            case "SMA":
                applySimpleMovingAverage(context, points);
                break;
            case "EWMA":
                applyExponentialWeightedMovingAverage(context, points);
                break;
            case "MEDIAN":
                applyMedianFilter(context, points);
                break;
            default:
                applySimpleMovingAverage(context, points);
        }
    }

    /**
     * 简单移动平均
     */
    private void applySimpleMovingAverage(OperatorContext context, List<DataPoint> points) {
        for (int i = 0; i < points.size(); i++) {
            DataPoint dp = points.get(i);
            int start = Math.max(0, i - windowSize + 1);

            double sum = 0;
            int count = 0;
            for (int j = start; j <= i; j++) {
                sum += ((Number) points.get(j).getValue()).doubleValue();
                count++;
            }

            double smoothed = sum / count;
            context.setOutputValue(dp.getTimestamp(), smoothed, dp.getQualityCode());
        }
    }

    /**
     * 指数加权移动平均
     */
    private void applyExponentialWeightedMovingAverage(OperatorContext context, List<DataPoint> points) {
        double ewma = ((Number) points.get(0).getValue()).doubleValue();
        context.setOutputValue(points.get(0).getTimestamp(), ewma, points.get(0).getQualityCode());

        for (int i = 1; i < points.size(); i++) {
            DataPoint dp = points.get(i);
            double value = ((Number) dp.getValue()).doubleValue();
            ewma = alpha * value + (1 - alpha) * ewma;
            context.setOutputValue(dp.getTimestamp(), ewma, dp.getQualityCode());
        }
    }

    /**
     * 中值滤波
     */
    private void applyMedianFilter(OperatorContext context, List<DataPoint> points) {
        for (int i = 0; i < points.size(); i++) {
            DataPoint dp = points.get(i);
            int start = Math.max(0, i - windowSize / 2);
            int end = Math.min(points.size() - 1, i + windowSize / 2);

            double[] window = new double[end - start + 1];
            for (int j = start; j <= end; j++) {
                window[j - start] = ((Number) points.get(j).getValue()).doubleValue();
            }
            Arrays.sort(window);

            double median = window[window.length / 2];
            context.setOutputValue(dp.getTimestamp(), median, dp.getQualityCode());
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public FunctionMetadata getMetadata() {
        FunctionMetadata meta = new FunctionMetadata();
        meta.setFunctionId("smoothing_filter");
        meta.setName("滤波平滑算子");
        meta.setVersion("1.0.0");
        meta.setDescription("基于滑动窗口对时序数据进行噪声平滑。"
                + "中值滤波对孤立脉冲噪声抑制效果好，EWMA在平滑性和响应速度之间可调。");
        meta.setApplicableTypes(Arrays.asList("PRESSURE", "TEMPERATURE", "FLOW", "VIBRATION"));

        ParameterDefinition typeDef = new ParameterDefinition();
        typeDef.setName("filterType");
        typeDef.setType("ENUM");
        typeDef.setRequired(false);
        typeDef.setDefaultValue("SMA");
        typeDef.setEnumValues(Arrays.asList("SMA", "EWMA", "MEDIAN"));
        typeDef.setDescription("滤波类型：简单移动平均/指数加权移动平均/中值滤波");

        ParameterDefinition windowDef = new ParameterDefinition();
        windowDef.setName("windowSize");
        windowDef.setType("NUMBER");
        windowDef.setRequired(false);
        windowDef.setDefaultValue(5.0);
        windowDef.setMinValue(2.0);
        windowDef.setMaxValue(500.0);
        windowDef.setDescription("滑动窗口大小");

        ParameterDefinition alphaDef = new ParameterDefinition();
        alphaDef.setName("alpha");
        alphaDef.setType("NUMBER");
        alphaDef.setRequired(false);
        alphaDef.setDefaultValue(0.3);
        alphaDef.setMinValue(0.01);
        alphaDef.setMaxValue(1.0);
        alphaDef.setDescription("EWMA衰减系数，值越大对新数据越敏感");

        meta.setParameterDefinitions(Arrays.asList(typeDef, windowDef, alphaDef));
        return meta;
    }
}
