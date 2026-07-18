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
 * {@link DtcPairPattern} Handler：dtc1 → dtc2（禁止硬读 harsh/fault 步骤名）。
 *
 * <p>MATCH/TIMEOUT 均写入 {@link PatternIds#DTC_PAIR}（D-08/D-09）。
 */
public final class DtcPairHandler
        extends PatternProcessFunction<VehicleEvent, AlertEvent>
        implements TimedOutPartialMatchHandler<VehicleEvent> {

    private final OutputTag<AlertEvent> timeoutTag;

    public DtcPairHandler() {
        this(AlertPatternHandler.TIMEOUT_TAG);
    }

    public DtcPairHandler(OutputTag<AlertEvent> timeoutTag) {
        this.timeoutTag = timeoutTag;
    }

    @Override
    public void processMatch(
            Map<String, List<VehicleEvent>> match,
            Context ctx,
            Collector<AlertEvent> out) {
        VehicleEvent dtc1 = match.get("dtc1").get(0);
        VehicleEvent dtc2 = match.get("dtc2").get(0);
        out.collect(new AlertEvent(
                dtc1.vin,
                "MATCH",
                0.0,
                dtc2.value,
                dtc2.eventTime,
                "15s 内出现重复故障码",
                PatternIds.DTC_PAIR));
    }

    @Override
    public void processTimedOutMatch(
            Map<String, List<VehicleEvent>> match,
            Context ctx) {
        List<VehicleEvent> dtc1Events = match.get("dtc1");
        if (dtc1Events == null || dtc1Events.isEmpty()) {
            return;
        }
        VehicleEvent dtc1 = dtc1Events.get(0);
        ctx.output(timeoutTag, new AlertEvent(
                dtc1.vin,
                "TIMEOUT",
                0.0,
                dtc1.value,
                dtc1.eventTime,
                "15s 内未出现第二次故障码",
                PatternIds.DTC_PAIR));
    }
}
