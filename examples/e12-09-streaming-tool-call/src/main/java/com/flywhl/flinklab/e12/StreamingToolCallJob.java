package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ai/第09章 Demo · Tool Call 降级路径：幂等键去重 + 副作用侧输出。
 *
 * <p>模拟"调用外部工具"(发券/写工单)。同一 userId+page 的动作只执行一次；
 * 真正副作用写入 Side Output，主流通过审计日志。Failover 重放时 ValueState
 * 已记录幂等键，副作用不会重复 —— 对应 Durable Execution 的工程底线。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-09-streaming-tool-call \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingToolCallJob
 */
public final class StreamingToolCallJob {
    private StreamingToolCallJob() {
    }

    public static final OutputTag<String> SIDE_EFFECT =
            new OutputTag<String>("tool-side-effect") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = Labs.events(env, "tool-calls", 25, 3, 12, 200);

        SingleOutputStreamOperator<String> main = events
                .keyBy(e -> e.userId + "|" + e.page)
                .process(new IdempotentToolInvoker())
                .uid("e12-09-tool-idempotent");

        main.print("AUDIT");
        main.getSideOutput(SIDE_EFFECT).print("SIDE-EFFECT");

        env.execute("e12-09-streaming-tool-call");
    }

    public static final class IdempotentToolInvoker
            extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Boolean> done;

        @Override
        public void open(OpenContext ctx) {
            done = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("tool-done", Boolean.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            String idemKey = e.userId + "|" + e.page;
            if (Boolean.TRUE.equals(done.value())) {
                out.collect("SKIP  idemKey=%s amount=%.1f (已执行)".formatted(idemKey, e.amount));
                return;
            }
            if (e.amount < 300) {
                out.collect("HOLD  idemKey=%s amount=%.1f (未达触发阈)".formatted(idemKey, e.amount));
                return;
            }
            done.update(true);
            ctx.output(SIDE_EFFECT,
                    "INVOKE tool=issue_coupon idemKey=%s amount=%.1f".formatted(idemKey, e.amount));
            out.collect("OK    idemKey=%s 副作用已侧输出".formatted(idemKey));
        }
    }
}
