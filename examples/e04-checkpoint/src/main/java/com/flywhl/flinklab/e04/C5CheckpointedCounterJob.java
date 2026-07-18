package com.flywhl.flinklab.e04;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * e04-C5 · 开启 checkpoint 的有状态计数：本地观察 WebUI checkpoint 完成次数。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e04-checkpoint \
 *          -Dexec.mainClass=com.flywhl.flinklab.e04.C5CheckpointedCounterJob
 */
public final class C5CheckpointedCounterJob {
    private C5CheckpointedCounterJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(Duration.ofSeconds(2).toMillis());
        Labs.events(env, "chk-cnt", 30, 3, 8, 100)
            .keyBy(e -> e.userId)
            .process(new Counter())
            .uid("e04-c5-counter")
            .print();
        env.execute("e04-c5-checkpointed-counter");
    }

    public static final class Counter extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Long> n;

        @Override
        public void open(OpenContext ctx) {
            n = getRuntimeContext().getState(new ValueStateDescriptor<>("n", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            long v = (n.value() == null ? 0L : n.value()) + 1;
            n.update(v);
            if (v % 10 == 0) {
                out.collect("CHK-CNT user=%s n=%d".formatted(e.userId, v));
            }
        }
    }
}
