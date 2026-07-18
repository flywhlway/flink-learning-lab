package com.flywhl.flinklab.p01.cost;

import com.flywhl.flinklab.p01.model.LogResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 预算熔断算子：接在 RuleTagger 之后、AsyncDataStream 之前（D-12）。
 *
 * <p>未超限 → 主输出（进入 Ollama Async）；达上限 → Side Output 标记
 * {@code ai_source=DEGRADED}，短路外呼并 {@code budget_trips++}。
 */
public final class BudgetGateFunction
        extends KeyedProcessFunction<String, LogResult, LogResult> {

    private static final long serialVersionUID = 1L;

    /** 超限降级旁路：不进 Async，直接与 AI 结果合流。 */
    public static final OutputTag<LogResult> DEGRADED_TAG =
            new OutputTag<>("p01-budget-degraded") {
            };

    private final int maxAiCalls;

    private transient ValueState<Long> callCountState;
    private transient BudgetGate gate;
    private transient Counter budgetTrips;

    public BudgetGateFunction(int maxAiCalls) {
        this.maxAiCalls = Math.max(0, maxAiCalls);
    }

    @Override
    public void open(OpenContext openContext) {
        callCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("p01-budget-ai-calls", Long.class));
        gate = new BudgetGate(maxAiCalls);
        MetricGroup g = getRuntimeContext().getMetricGroup().addGroup("p01");
        budgetTrips = g.counter("budget_trips");
    }

    @Override
    public void processElement(LogResult value, Context ctx, Collector<LogResult> out)
            throws Exception {
        if (value == null) {
            return;
        }
        long calls = callCountState.value() == null ? 0L : callCountState.value();
        int callsInt = calls > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) calls;

        if (gate.allow(callsInt)) {
            callCountState.update(calls + 1);
            out.collect(value);
            return;
        }

        budgetTrips.inc();
        LogResult degraded = copyOf(value);
        degraded.aiSource = "DEGRADED";
        if (degraded.aiRisk == null
                || degraded.aiRisk.isBlank()
                || "NONE".equals(degraded.aiRisk)) {
            degraded.aiRisk = "UNKNOWN";
        }
        // 规则标签保留；短路 AI 外呼
        ctx.output(DEGRADED_TAG, degraded);
    }

    static LogResult copyOf(LogResult src) {
        return new LogResult(
                src.service,
                src.level,
                src.message,
                src.traceId,
                src.eventTime,
                src.featureJson,
                src.ruleLabel,
                src.aiRisk,
                src.aiSource);
    }
}
