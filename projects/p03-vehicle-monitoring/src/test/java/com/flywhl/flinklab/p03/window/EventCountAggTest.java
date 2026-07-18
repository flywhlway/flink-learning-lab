package com.flywhl.flinklab.p03.window;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 窗口累加器契约（D-02 / RESEARCH Pattern 3）：event / harsh / dtc 三分计数。
 *
 * <p>轻量单测：无 MiniCluster；直接断言 {@link EventCountAgg} 的 create / add / merge。
 *
 * <p>Wave 0 RED：故意引用尚未交付的 {@code EventCountAgg}（由 03-01 GREEN）。
 */
class EventCountAggTest {

    @Test
    void createAccumulatorIsZero() {
        EventCountAgg agg = new EventCountAgg();
        EventCountAgg.Counts acc = agg.createAccumulator();
        assertEquals(0L, acc.eventCount, "新累加器 eventCount 须为 0");
        assertEquals(0L, acc.harshCount, "新累加器 harshCount 须为 0");
        assertEquals(0L, acc.dtcCount, "新累加器 dtcCount 须为 0");
    }

    @Test
    void heartbeatIncrementsEventCountOnly() {
        EventCountAgg agg = new EventCountAgg();
        EventCountAgg.Counts acc = agg.createAccumulator();
        agg.add(new VehicleEvent("VIN-1", "HEARTBEAT", 1.0, 1_000L), acc);
        assertEquals(1L, acc.eventCount, "HEARTBEAT 须增 eventCount");
        assertEquals(0L, acc.harshCount, "HEARTBEAT 不得增 harshCount");
        assertEquals(0L, acc.dtcCount, "HEARTBEAT 不得增 dtcCount");
    }

    @Test
    void harshAccelIncrementsEventAndHarsh() {
        EventCountAgg agg = new EventCountAgg();
        EventCountAgg.Counts acc = agg.createAccumulator();
        agg.add(new VehicleEvent("VIN-1", "HARSH_ACCEL", 451.0, 2_000L), acc);
        assertEquals(1L, acc.eventCount, "HARSH_ACCEL 须增 eventCount");
        assertEquals(1L, acc.harshCount, "HARSH_ACCEL 须增 harshCount");
        assertEquals(0L, acc.dtcCount, "HARSH_ACCEL 不得增 dtcCount");
    }

    @Test
    void dtcIncrementsEventAndDtc() {
        EventCountAgg agg = new EventCountAgg();
        EventCountAgg.Counts acc = agg.createAccumulator();
        agg.add(new VehicleEvent("VIN-1", "DTC", 481.0, 3_000L), acc);
        assertEquals(1L, acc.eventCount, "DTC 须增 eventCount");
        assertEquals(0L, acc.harshCount, "DTC 不得增 harshCount");
        assertEquals(1L, acc.dtcCount, "DTC 须增 dtcCount");
    }

    @Test
    void mergeAddsCounts() {
        EventCountAgg agg = new EventCountAgg();
        EventCountAgg.Counts left = agg.createAccumulator();
        EventCountAgg.Counts right = agg.createAccumulator();
        agg.add(new VehicleEvent("VIN-1", "HARSH_ACCEL", 451.0, 1_000L), left);
        agg.add(new VehicleEvent("VIN-1", "DTC", 481.0, 2_000L), right);
        agg.add(new VehicleEvent("VIN-1", "HEARTBEAT", 1.0, 3_000L), right);

        EventCountAgg.Counts merged = agg.merge(left, right);
        assertEquals(3L, merged.eventCount, "merge 后 eventCount 须相加");
        assertEquals(1L, merged.harshCount, "merge 后 harshCount 须相加");
        assertEquals(1L, merged.dtcCount, "merge 后 dtcCount 须相加");
    }
}
