package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** e12-09 第二 Job · 每用户工具调用速率限制（处理时间滑动计数）。 */
public final class StreamingToolCallRateLimitJob {
    private StreamingToolCallRateLimitJob() {}
    private static final int LIMIT = 3;
    private static final long WINDOW_MS = 5_000;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "tool-rl", 20, 3, 12, 100)
            .keyBy(e -> e.userId)
            .process(new RateLimit())
            .uid("e12-09-tool-ratelimit")
            .print();
        env.execute("e12-09-streaming-tool-call-ratelimit");
    }

    public static final class RateLimit extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Integer> cnt;
        private transient ValueState<Long> windowStart;

        @Override
        public void open(OpenContext ctx) {
            cnt = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Integer.class));
            windowStart = getRuntimeContext().getState(new ValueStateDescriptor<>("ws", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            long now = ctx.timerService().currentProcessingTime();
            Long ws = windowStart.value();
            if (ws == null || now - ws >= WINDOW_MS) {
                ws = now;
                windowStart.update(ws);
                cnt.update(0);
            }
            int c = (cnt.value() == null ? 0 : cnt.value()) + 1;
            cnt.update(c);
            if (c > LIMIT) {
                out.collect("RATE-LIMIT user=%s cnt=%d".formatted(e.userId, c));
            } else {
                out.collect("ALLOW user=%s cnt=%d".formatted(e.userId, c));
            }
        }
    }
}
