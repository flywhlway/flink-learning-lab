package com.flywhl.flinklab.e02;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * e02-C6 · 滑动事件时间窗口：size=10s slide=5s，观察重叠窗口重复计数。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e02-time-window \
 *          -Dexec.mainClass=com.flywhl.flinklab.e02.C6SlidingEventTimeJob
 */
public final class C6SlidingEventTimeJob {
    private C6SlidingEventTimeJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> events = Labs.events(env, "slide", 20, 3, 5, 400)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(500))
                                .withTimestampAssigner((e, ts) -> e.ts));
        events.keyBy(e -> e.userId)
              .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
              .process(new SlideCount())
              .uid("e02-c6-slide")
              .print();
        env.execute("e02-c6-sliding-event-time");
    }

    public static final class SlideCount
            extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String user, Context ctx, Iterable<Event> elements, Collector<String> out) {
            long n = 0;
            for (Event ignored : elements) {
                n++;
            }
            out.collect("SLIDE user=%s n=%d win=[%d,%d)"
                    .formatted(user, n, ctx.window().getStart(), ctx.window().getEnd()));
        }
    }
}
