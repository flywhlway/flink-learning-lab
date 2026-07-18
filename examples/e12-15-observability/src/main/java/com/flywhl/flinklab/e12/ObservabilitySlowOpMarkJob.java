package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** e12-15 第二 Job · 慢事件标记：处理延迟超阈打 SLOW，教学结构化日志字段。 */
public final class ObservabilitySlowOpMarkJob {
    private ObservabilitySlowOpMarkJob() {}

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "obs-slow", 25, 4, 5, 800)
            .process(new SlowMark())
            .uid("e12-15-slow-mark")
            .print();
        env.execute("e12-15-observability-slow-mark");
    }

    public static final class SlowMark extends ProcessFunction<Event, String> {
        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) {
            long lag = System.currentTimeMillis() - e.ts;
            String level = lag > 500 ? "SLOW" : "OK";
            out.collect("%s lagMs=%d user=%s page=%s".formatted(level, lag, e.userId, e.page));
        }
    }
}
