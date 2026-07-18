package com.flywhl.flinklab.p03.window;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 按 vin 窗口的三分计数累加器：event / harsh / dtc（D-02 / RESEARCH Pattern 3）。
 *
 * <p>signalType 白名单与 {@code ParseVehicleJson} 对齐：HEARTBEAT / HARSH_ACCEL / DTC。
 */
public final class EventCountAgg
        implements AggregateFunction<VehicleEvent, EventCountAgg.Counts, EventCountAgg.Counts> {

    @Override
    public Counts createAccumulator() {
        return new Counts();
    }

    @Override
    public Counts add(VehicleEvent event, Counts acc) {
        acc.eventCount++;
        if ("HARSH_ACCEL".equals(event.signalType)) {
            acc.harshCount++;
        }
        if ("DTC".equals(event.signalType)) {
            acc.dtcCount++;
        }
        return acc;
    }

    @Override
    public Counts getResult(Counts acc) {
        return acc;
    }

    @Override
    public Counts merge(Counts a, Counts b) {
        a.eventCount += b.eventCount;
        a.harshCount += b.harshCount;
        a.dtcCount += b.dtcCount;
        return a;
    }

    /** 窗口累加状态；字段名与 {@code EventCountAggTest} / CH 列语义对齐。 */
    public static final class Counts implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public long eventCount;
        public long harshCount;
        public long dtcCount;
    }
}
