package com.flywhl.flinklab.p03.window;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 将累加器结果贴上窗口起止时间与 vin，输出 {@link WindowMetricsRow}。
 */
public final class AttachWindowMeta
        extends ProcessWindowFunction<EventCountAgg.Counts, WindowMetricsRow, String, TimeWindow> {

    @Override
    public void process(
            String vin,
            Context ctx,
            Iterable<EventCountAgg.Counts> counts,
            Collector<WindowMetricsRow> out) {
        EventCountAgg.Counts c = counts.iterator().next();
        out.collect(new WindowMetricsRow(
                vin,
                ctx.window().getStart(),
                ctx.window().getEnd(),
                c.eventCount,
                c.harshCount,
                c.dtcCount));
    }
}
