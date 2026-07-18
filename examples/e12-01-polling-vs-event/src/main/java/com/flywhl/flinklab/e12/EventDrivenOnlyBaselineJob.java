package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** e12-01 第二 Job · 仅事件驱动基线，便于与主 Job 轮询分支对照压测。 */
public final class EventDrivenOnlyBaselineJob {
    private EventDrivenOnlyBaselineJob() {}
    private static final double TH = 450;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "ed-only", 30, 5, 8, 200)
            .process(new Detector())
            .uid("e12-01-event-only")
            .print();
        env.execute("e12-01-event-driven-only-baseline");
    }

    public static final class Detector extends ProcessFunction<Event, String> {
        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) {
            if (e.amount > TH) {
                long now = System.currentTimeMillis();
                out.collect("BASELINE user=%s delay=%dms".formatted(e.userId, now - e.ts));
            }
        }
    }
}
