package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.AlertEvent;
import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * harsh→fault 匹配主流 + 超时半成品 Side Output（对齐 e10-C3）。
 *
 * <p>MATCH 走 Collector 主流；TIMEOUT 经 {@code ctx.output(timeoutTag, ...)}，
 * 同表 {@code alert_type} 语义（Open Questions RESOLVED Q3）。
 */
public final class AlertPatternHandler
        extends PatternProcessFunction<VehicleEvent, AlertEvent>
        implements TimedOutPartialMatchHandler<VehicleEvent> {

    public static final OutputTag<AlertEvent> TIMEOUT_TAG =
            new OutputTag<>("p03-timeout-alerts") {
            };

    private final OutputTag<AlertEvent> timeoutTag;

    public AlertPatternHandler() {
        this(TIMEOUT_TAG);
    }

    public AlertPatternHandler(OutputTag<AlertEvent> timeoutTag) {
        this.timeoutTag = timeoutTag;
    }

    @Override
    public void processMatch(
            Map<String, List<VehicleEvent>> match,
            Context ctx,
            Collector<AlertEvent> out) {
        VehicleEvent harsh = match.get("harsh").get(0);
        VehicleEvent fault = match.get("fault").get(0);
        out.collect(new AlertEvent(
                harsh.vin,
                "MATCH",
                harsh.value,
                fault.value,
                fault.eventTime,
                "急加速后出现故障信号"));
    }

    @Override
    public void processTimedOutMatch(
            Map<String, List<VehicleEvent>> match,
            Context ctx) {
        List<VehicleEvent> harshEvents = match.get("harsh");
        if (harshEvents == null || harshEvents.isEmpty()) {
            return;
        }
        VehicleEvent harsh = harshEvents.get(0);
        ctx.output(timeoutTag, new AlertEvent(
                harsh.vin,
                "TIMEOUT",
                harsh.value,
                0.0,
                harsh.eventTime,
                "急加速后 30s 内未出现故障信号"));
    }
}
