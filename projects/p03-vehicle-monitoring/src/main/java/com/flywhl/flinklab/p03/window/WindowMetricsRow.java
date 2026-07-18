package com.flywhl.flinklab.p03.window;

import java.io.Serializable;

/**
 * 窗口聚合结果行：写入 {@code flinklab.vehicle_window_metrics}。
 */
public final class WindowMetricsRow implements Serializable {

    private static final long serialVersionUID = 1L;

    public String vin;
    public long windowStart;
    public long windowEnd;
    public long eventCount;
    public long harshCount;
    public long dtcCount;

    public WindowMetricsRow() {
    }

    public WindowMetricsRow(
            String vin,
            long windowStart,
            long windowEnd,
            long eventCount,
            long harshCount,
            long dtcCount) {
        this.vin = vin;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.eventCount = eventCount;
        this.harshCount = harshCount;
        this.dtcCount = dtcCount;
    }
}
