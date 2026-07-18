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
 * e08-C6 · 软删除 / tombstone 模拟：amount 低于 50 视为删除标记，清状态并输出 -D。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e08-cdc \
 *          -Dexec.mainClass=com.flywhl.flinklab.e08.C6SoftDeleteTombstoneJob
 */
public final class C6SoftDeleteTombstoneJob {
    private C6SoftDeleteTombstoneJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "tombstone", 18, 3, 8, 200)
            .keyBy(e -> e.userId)
            .process(new TombstoneFn())
            .uid("e08-c6-tombstone")
            .print();
        env.execute("e08-c6-soft-delete-tombstone");
    }

    public static final class TombstoneFn extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<String> live;

        @Override
        public void open(OpenContext ctx) {
            live = getRuntimeContext().getState(new ValueStateDescriptor<>("live", String.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            if (e.amount < 50) {
                if (live.value() != null) {
                    out.collect("DELETE op=-D pk=%s reason=soft-delete".formatted(e.userId));
                    live.clear();
                } else {
                    out.collect("IGNORE tombstone pk=%s (无存活行)".formatted(e.userId));
                }
                return;
            }
            live.update(e.page);
            out.collect("UPSERT pk=%s page=%s amount=%.1f".formatted(e.userId, e.page, e.amount));
        }
    }
}
