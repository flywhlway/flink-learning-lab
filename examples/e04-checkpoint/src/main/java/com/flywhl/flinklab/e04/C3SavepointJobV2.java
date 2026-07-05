package com.flywhl.flinklab.e04;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e04-C3(V2) · "新版本"作业:同一状态(uid=e04-c3-count, name=total)+ 新逻辑。
 * 从 V1 的 savepoint 恢复后,输出前缀变 v2、附带分级标签,且 total 延续 V1 的值。
 * 新增的无状态算子(tag-tier)不在 savepoint 里,恢复不受影响。
 */
public final class C3SavepointJobV2 {
    private C3SavepointJobV2() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        Labs.events(env, "traffic", 50, 8, 8, 500)
            .keyBy(e -> e.userId)
            .process(new CountFnV2())
            .uid("e04-c3-count")                 // ← 与 V1 一致,状态得以延续
            .map(s -> s + (s.endsWith("0") ? "  [整十里程碑]" : ""))
            .name("tag-tier").uid("e04-c3-tier") // 新增无状态算子,自由添加
            .print();

        env.execute("e04-c3-savepoint-v2");
    }

    /** V2 逻辑:同名状态 + 升级后的输出。 */
    public static class CountFnV2 extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Long> total;

        @Override
        public void open(OpenContext ctx) {
            total = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("total", Long.class)); // 状态名也保持一致
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out)
                throws Exception {
            long t = (total.value() == null ? 0 : total.value()) + 1;
            total.update(t);
            out.collect("v2 user=%s total=%d".formatted(e.userId, t));
        }
    }
}
