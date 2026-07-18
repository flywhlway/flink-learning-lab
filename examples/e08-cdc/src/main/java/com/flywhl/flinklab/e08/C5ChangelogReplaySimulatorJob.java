package com.flywhl.flinklab.e08;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e08-C5 · CDC changelog 语义模拟（零 Postgres）：用 Labs 事件模拟 upsert 流。
 *
 * <p>同一 userId 的连续事件视为同一主键的变更；首次 INSERT，之后 UPDATE。
 * 教学点：CDC 进入 Flink 后的算子侧心智是 changelog，不必每次都连真库。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e08-cdc \
 *          -Dexec.mainClass=com.flywhl.flinklab.e08.C5ChangelogReplaySimulatorJob
 */
public final class C5ChangelogReplaySimulatorJob {
    private C5ChangelogReplaySimulatorJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "cdc-sim", 15, 4, 6, 200)
            .keyBy(e -> e.userId)
            .process(new ChangelogFn())
            .uid("e08-c5-changelog")
            .print();
        env.execute("e08-c5-changelog-replay-simulator");
    }

    public static final class ChangelogFn extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Double> last;

        @Override
        public void open(OpenContext ctx) {
            last = getRuntimeContext().getState(new ValueStateDescriptor<>("row", Double.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            Double prev = last.value();
            if (prev == null) {
                out.collect("INSERT op=+I pk=%s amount=%.1f".formatted(e.userId, e.amount));
            } else {
                out.collect("UPDATE op=+U pk=%s before=%.1f after=%.1f"
                        .formatted(e.userId, prev, e.amount));
            }
            last.update(e.amount);
        }
    }
}
