package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e03-C1 · ValueState:每用户累计消费额(状态的 Hello World)。
 *
 * <p>要点:状态句柄在 open() 里获取一次;运行期读写的"当前 key"由框架根据
 * 正在处理的元素自动切换 —— 你永远不用、也不能自己传 key。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e03-state \
 *          -Dexec.mainClass=com.flywhl.flinklab.e03.C1ValueStateBalanceJob
 */
public final class C1ValueStateBalanceJob {
    private C1ValueStateBalanceJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 并行度 >1:观察同一 key 恒定落在同一 subtask

        Labs.events(env, "spend", 20, 6, 5, 1_000)
            .keyBy(e -> e.userId)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private transient ValueState<Double> total;

                @Override
                public void open(OpenContext ctx) {
                    total = getRuntimeContext().getState(
                            new ValueStateDescriptor<>("total-spend", Double.class));
                }

                @Override
                public void processElement(Event e, Context ctx, Collector<String> out)
                        throws Exception {
                    double t = (total.value() == null ? 0 : total.value()) + e.amount;
                    total.update(t);
                    out.collect("subtask=%d user=%s total=%.1f".formatted(
                            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                            e.userId, t));
                }
            })
            .uid("e03-c1-balance")
            .print();

        env.execute("e03-c1-value-state-balance");
    }
}
