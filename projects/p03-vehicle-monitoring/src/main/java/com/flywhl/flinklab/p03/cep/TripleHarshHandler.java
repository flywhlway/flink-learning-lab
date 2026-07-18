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
 * {@link TripleHarshPattern} Handler：连续三次急加速（步骤名 {@code harsh}，禁止硬读 fault）。
 *
 * <p>MATCH/TIMEOUT 均写入 {@link PatternIds#TRIPLE_HARSH}（D-08/D-09）。
 */
public final class TripleHarshHandler
        extends PatternProcessFunction<VehicleEvent, AlertEvent>
        implements TimedOutPartialMatchHandler<VehicleEvent> {

    private final OutputTag<AlertEvent> timeoutTag;

    public TripleHarshHandler() {
        this(AlertPatternHandler.TIMEOUT_TAG);
    }

    public TripleHarshHandler(OutputTag<AlertEvent> timeoutTag) {
        this.timeoutTag = timeoutTag;
    }

    @Override
    public void processMatch(
            Map<String, List<VehicleEvent>> match,
            Context ctx,
            Collector<AlertEvent> out) {
        List<VehicleEvent> harshEvents = match.get("harsh");
        if (harshEvents == null || harshEvents.isEmpty()) {
            return;
        }
        VehicleEvent first = harshEvents.get(0);
        VehicleEvent last = harshEvents.get(harshEvents.size() - 1);
        out.collect(new AlertEvent(
                first.vin,
                "MATCH",
                last.value,
                0.0,
                last.eventTime,
                "20s 内连续 3 次急加速",
                PatternIds.TRIPLE_HARSH));
    }

    @Override
    public void processTimedOutMatch(
            Map<String, List<VehicleEvent>> match,
            Context ctx) {
        List<VehicleEvent> harshEvents = match.get("harsh");
        if (harshEvents == null || harshEvents.isEmpty()) {
            return;
        }
        VehicleEvent first = harshEvents.get(0);
        VehicleEvent last = harshEvents.get(harshEvents.size() - 1);
        ctx.output(timeoutTag, new AlertEvent(
                first.vin,
                "TIMEOUT",
                last.value,
                0.0,
                last.eventTime,
                "20s 内未凑满 3 次急加速",
                PatternIds.TRIPLE_HARSH));
    }
}
