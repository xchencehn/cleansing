package com.pipeline.cleansing.operators;

import com.pipeline.cleansing.core.OperatorContext;
import com.pipeline.cleansing.core.UFunction;
import com.pipeline.cleansing.model.*;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * 插值填充算子。
 * 对检测到的数据缺失区间执行补全。
 *
 * 参数：
 * - expectedIntervalMs: 预期采集间隔（毫秒）(NUMBER, 必选)
 * - method: 插值方法 (ENUM: LINEAR / ZERO_ORDER_HOLD / CUBIC_SPLINE, 默认 LINEAR)
 * - maxGapMs: 最大可填充缺失时长（毫秒）(NUMBER, 默认 300000即5分钟)
 * - longGapMethod: 长时间缺失的填充方法 (ENUM: ZERO_ORDER_HOLD / SKIP, 默认 ZERO_ORDER_HOLD)
 */
public class InterpolationFillOperator implements UFunction {

    private long expectedIntervalMs;
    private String method;
    private long maxGapMs;
    private String longGapMethod;

    @Override
    public void initialize(OperatorContext context) {
        this.expectedIntervalMs = context.getParameter("expectedIntervalMs", 5000L);
        this.method = context.getParameter("method", "LINEAR");
        this.maxGapMs = context.getParameter("maxGapMs", 300000L);
        this.longGapMethod = context.getParameter("longGapMethod", "ZERO_ORDER_HOLD");
    }

    @Override
    public void execute(OperatorContext context) {
        TimeSeriesData input = context.getInputData(null);
        if (input == null || input.isEmpty()) return;

        List<DataPoint> points = input.getDataPoints();

        // 输出第一个点
        DataPoint first = points.get(0);
        context.setOutputValue(first.getTimestamp(), first.getValue(), first.getQualityCode());

        for (int i = 1; i < points.size(); i++) {
            DataPoint prev = points.get(i - 1);
            DataPoint curr = points.get(i);

            long gap = curr.getTimestamp().getTime() - prev.getTimestamp().getTime();

            if (gap > expectedIntervalMs * 1.5) {
                // 检测到缺失区间，执行填充
                fillGap(context, prev, curr, gap);
            }

            // 输出当前点
            context.setOutputValue(curr.getTimestamp(), curr.getValue(), curr.getQualityCode());
        }
    }

    private void fillGap(OperatorContext context, DataPoint before, DataPoint after, long gapMs) {
        String effectiveMethod = (gapMs > maxGapMs) ? longGapMethod : method;

        if ("SKIP".equals(effectiveMethod)) {
            return; // 跳过不填充
        }

        long startMs = before.getTimestamp().getTime() + expectedIntervalMs;
        long endMs = after.getTimestamp().getTime();

        double beforeVal = ((Number) before.getValue()).doubleValue();
        double afterVal = ((Number) after.getValue()).doubleValue();

        for (long ts = startMs; ts < endMs; ts += expectedIntervalMs) {
            double interpolatedValue;

            switch (effectiveMethod) {
                case "LINEAR":
                    // 线性插值
                    double ratio = (double) (ts - before.getTimestamp().getTime())
                            / (after.getTimestamp().getTime() - before.getTimestamp().getTime());
                    interpolatedValue = beforeVal + ratio * (afterVal - beforeVal);
                    break;

                case "ZERO_ORDER_HOLD":
                    // 零阶保持
                    interpolatedValue = beforeVal;
                    break;

                case "CUBIC_SPLINE":
                    // 简化的三次插值（使用Hermite插值近似）
                    double t = (double) (ts - before.getTimestamp().getTime())
                            / (after.getTimestamp().getTime() - before.getTimestamp().getTime());
                    interpolatedValue = hermiteInterpolate(beforeVal, afterVal, t);
                    break;

                default:
                    interpolatedValue = beforeVal;
            }

            // 质量码标记为插值生成（64 = 插值标记）
            context.setOutputValue(new Timestamp(ts), interpolatedValue, 64);
        }
    }

    /**
     * Hermite插值近似三次样条
     */
    private double hermiteInterpolate(double y0, double y1, double t) {
        double t2 = t * t;
        double t3 = t2 * t;
        double h00 = 2 * t3 - 3 * t2 + 1;
        double h01 = -2 * t3 + 3 * t2;
        // 端点斜率设为0（自然边界条件）
        return h00 * y0 + h01 * y1;
    }

    @Override
    public void cleanup() {}

    @Override
    public FunctionMetadata getMetadata() {
        FunctionMetadata meta = new FunctionMetadata();
        meta.setFunctionId("interpolation_fill");
        meta.setName("插值填充算子");
        meta.setVersion("1.0.0");
        meta.setDescription("对检测到的数据缺失区间执行补全，支持线性插值、零阶保持和三次样条插值。"
                + "可根据缺失时长自动选择策略。");
        meta.setApplicableTypes(Arrays.asList("PRESSURE", "TEMPERATURE", "FLOW"));

        ParameterDefinition intervalDef = new ParameterDefinition();
        intervalDef.setName("expectedIntervalMs");
        intervalDef.setType("NUMBER");
        intervalDef.setRequired(true);
        intervalDef.setMinValue(1000.0);
        intervalDef.setMaxValue(60000.0);
        intervalDef.setDescription("预期采集间隔（毫秒）");

        ParameterDefinition methodDef = new ParameterDefinition();
        methodDef.setName("method");
        methodDef.setType("ENUM");
        methodDef.setRequired(false);
        methodDef.setDefaultValue("LINEAR");
        methodDef.setEnumValues(Arrays.asList("LINEAR", "ZERO_ORDER_HOLD", "CUBIC_SPLINE"));
        methodDef.setDescription("插值方法");

        ParameterDefinition maxGapDef = new ParameterDefinition();
        maxGapDef.setName("maxGapMs");
        maxGapDef.setType("NUMBER");
        maxGapDef.setRequired(false);
        maxGapDef.setDefaultValue(300000.0);
        maxGapDef.setDescription("最大可填充缺失时长（毫秒），超过此时长使用longGapMethod");

        ParameterDefinition longGapDef = new ParameterDefinition();
        longGapDef.setName("longGapMethod");
        longGapDef.setType("ENUM");
        longGapDef.setRequired(false);
        longGapDef.setDefaultValue("ZERO_ORDER_HOLD");
        longGapDef.setEnumValues(Arrays.asList("ZERO_ORDER_HOLD", "SKIP"));
        longGapDef.setDescription("长时间缺失的处理方法");

        meta.setParameterDefinitions(Arrays.asList(intervalDef, methodDef, maxGapDef, longGapDef));
        return meta;
    }
}
