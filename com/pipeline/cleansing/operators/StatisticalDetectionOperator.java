package com.pipeline.cleansing.operators;

import com.pipeline.cleansing.core.OperatorContext;
import com.pipeline.cleansing.core.UFunction;
import com.pipeline.cleansing.model.*;

import java.util.Arrays;
import java.util.List;

/**
 * 统计检测算子。
 * 基于时间窗口内的统计特征进行异常检测。
 * 将偏离均值超过指定倍数标准差的数据点标记为异常。
 *
 * 参数：
 * - sigmaMultiplier: 标准差倍数 (NUMBER, 默认3.0)
 * - windowSize: 统计窗口大小 (NUMBER, 默认30)
 * - action: 异常处理方式 (ENUM: MARK / REPLACE / REMOVE, 默认 MARK)
 */
public class StatisticalDetectionOperator implements UFunction {

    private double sigmaMultiplier;
    private int windowSize;
    private String action;

    @Override
    public void initialize(OperatorContext context) {
        this.sigmaMultiplier = context.getParameter("sigmaMultiplier", 3.0);
        this.windowSize = context.getParameter("windowSize", 30);
        this.action = context.getParameter("action", "MARK");
    }

    @Override
    public void execute(OperatorContext context) {
        TimeSeriesData input = context.getInputData(null);
        if (input == null || input.isEmpty()) return;

        List<DataPoint> points = input.getDataPoints();

        for (int i = 0; i < points.size(); i++) {
            DataPoint dp = points.get(i);
            double value = ((Number) dp.getValue()).doubleValue();

            // 计算当前窗口的统计特征
            int start = Math.max(0, i - windowSize + 1);
            double[] window = new double[i - start + 1];
            for (int j = start; j <= i; j++) {
                window[j - start] = ((Number) points.get(j).getValue()).doubleValue();
            }

            double mean = computeMean(window);
            double std = computeStd(window, mean);

            // 窗口太小或标准差为0时不做检测
            if (window.length < 3 || std == 0.0) {
                context.setOutputValue(dp.getTimestamp(), value, dp.getQualityCode());
                continue;
            }

            double deviation = Math.abs(value - mean);
            boolean isAnomaly = deviation > sigmaMultiplier * std;

            if (isAnomaly) {
                switch (action) {
                    case "MARK":
                        // 仅标记质量码，保留原值
                        context.setOutputValue(dp.getTimestamp(), value, 128); // 128=可疑标记
                        break;
                    case "REPLACE":
                        // 替换为窗口均值
                        context.setOutputValue(dp.getTimestamp(), mean, 192); // 192=修正标记
                        break;
                    case "REMOVE":
                        // 不输出该点（从序列中移除）
                        break;
                    default:
                        context.setOutputValue(dp.getTimestamp(), value, 128);
                }
            } else {
                context.setOutputValue(dp.getTimestamp(), value, dp.getQualityCode());
            }
        }
    }

    private double computeMean(double[] values) {
        double sum = 0;
        for (double v : values) sum += v;
        return sum / values.length;
    }

    private double computeStd(double[] values, double mean) {
        double sumSquares = 0;
        for (double v : values) {
            double diff = v - mean;
            sumSquares += diff * diff;
        }
        return Math.sqrt(sumSquares / values.length);
    }

    @Override
    public void cleanup() {}

    @Override
    public FunctionMetadata getMetadata() {
        FunctionMetadata meta = new FunctionMetadata();
        meta.setFunctionId("statistical_detection");
        meta.setName("统计检测算子");
        meta.setVersion("1.0.0");
        meta.setDescription("基于时间窗口内的均值和标准差进行异常检测。"
                + "对传感器精度漂移等渐变型异常有较好检测能力。");
        meta.setApplicableTypes(Arrays.asList("PRESSURE", "TEMPERATURE", "FLOW", "VIBRATION"));

        ParameterDefinition sigmaDef = new ParameterDefinition();
        sigmaDef.setName("sigmaMultiplier");
        sigmaDef.setType("NUMBER");
        sigmaDef.setRequired(false);
        sigmaDef.setDefaultValue(3.0);
        sigmaDef.setMinValue(1.0);
        sigmaDef.setMaxValue(10.0);
        sigmaDef.setDescription("标准差倍数，偏离均值超过此倍标准差判定为异常");

        ParameterDefinition windowDef = new ParameterDefinition();
        windowDef.setName("windowSize");
        windowDef.setType("NUMBER");
        windowDef.setRequired(false);
        windowDef.setDefaultValue(30.0);
        windowDef.setMinValue(5.0);
        windowDef.setMaxValue(1000.0);
        windowDef.setDescription("统计窗口大小");

        ParameterDefinition actionDef = new ParameterDefinition();
        actionDef.setName("action");
        actionDef.setType("ENUM");
        actionDef.setRequired(false);
        actionDef.setDefaultValue("MARK");
        actionDef.setEnumValues(Arrays.asList("MARK", "REPLACE", "REMOVE"));
        actionDef.setDescription("异常处理方式：标记/替换为均值/移除");

        meta.setParameterDefinitions(Arrays.asList(sigmaDef, windowDef, actionDef));
        return meta;
    }
}
