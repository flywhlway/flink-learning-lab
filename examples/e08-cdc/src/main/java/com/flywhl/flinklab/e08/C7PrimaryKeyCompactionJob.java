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
 * e08-C7 · 主键压缩：同一 pk 在处理时间窗口内多次变更只向外发最新快照。
 *
 * <p>模拟 CDC→下游前的 compact，降低重复 upsert 压力（不连真库）。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e08-cdc \
 *          -Dexec.mainClass=com.flywhl.flinklab.e08.C7PrimaryKeyCompactionJob
 */
public final class C7PrimaryKeyCompactionJob {
    private C7PrimaryKeyCompactionJob() {
    }

    private static final long COMPACT_MS = 2_000;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "compact", 25, 3, 10, 150)
            .keyBy(e -> e.userId)
            .process(new CompactFn())
            .uid("e08-c7-compact")
            .print();
        env.execute("e08-c7-pk-compaction");
    }

    public static final class CompactFn extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Event> pending;
        private transient ValueState<Long> timer;

        @Override
        public void open(OpenContext ctx) {
            pending = getRuntimeContext().getState(new ValueStateDescriptor<>("pending", Event.class));
            timer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            pending.update(e);
            Long t = timer.value();
            if (t == null) {
                long fire = ctx.timerService().currentProcessingTime() + COMPACT_MS;
                timer.update(fire);
                ctx.timerService().registerProcessingTimeTimer(fire);
                out.collect("BUFFER pk=%s (等待压缩)".formatted(e.userId));
            } else {
                out.collect("COALESCE pk=%s amount=%.1f".formatted(e.userId, e.amount));
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Event e = pending.value();
            if (e != null) {
                out.collect("FLUSH  pk=%s page=%s amount=%.1f".formatted(e.userId, e.page, e.amount));
            }
            pending.clear();
            timer.clear();
        }
    }
}
