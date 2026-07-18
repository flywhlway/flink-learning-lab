package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ai/第12章 Demo · Multi-Agent 协作：感知流 + 决策流 connect。
 *
 * <p>Agent-A(感知)写入最新信号；Agent-B(决策)在信号就绪后输出动作。
 * 用两条 Labs 流模拟不同角色，证明多 Agent 拓扑的本质是「共享 key 上的状态协作」，
 * 而非进程内共享内存。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-12-multi-agent-topology \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.MultiAgentTopologyJob
 */
public final class MultiAgentTopologyJob {
    private MultiAgentTopologyJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> sense = Labs.events(env, "agent-sense", 12, 3, 5, 200);
        DataStream<Event> decide = Labs.events(env, "agent-decide", 8, 3, 5, 200);

        sense.keyBy(e -> e.userId)
             .connect(decide.keyBy(e -> e.userId))
             .process(new CollaborateFn())
             .uid("e12-12-collaborate")
             .print();

        env.execute("e12-12-multi-agent-topology");
    }

    public static final class CollaborateFn
            extends KeyedCoProcessFunction<String, Event, Event, String> {
        private transient ValueState<String> lastSignal;
        private transient ValueState<Double> lastStrength;

        @Override
        public void open(OpenContext ctx) {
            lastSignal = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("signal", String.class));
            lastStrength = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("strength", Double.class));
        }

        @Override
        public void processElement1(Event sense, Context ctx, Collector<String> out) throws Exception {
            lastSignal.update(sense.page);
            lastStrength.update(sense.amount);
            out.collect("SENSE  user=%s signal=%s strength=%.1f"
                    .formatted(sense.userId, sense.page, sense.amount));
        }

        @Override
        public void processElement2(Event decide, Context ctx, Collector<String> out) throws Exception {
            String sig = lastSignal.value();
            Double strength = lastStrength.value();
            if (sig == null || strength == null) {
                out.collect("WAIT   user=%s decidePage=%s (感知未就绪)"
                        .formatted(decide.userId, decide.page));
                return;
            }
            String action = strength > 350 ? "ESCALATE" : "ACK";
            out.collect("DECIDE user=%s action=%s basedOn=%s strength=%.1f"
                    .formatted(decide.userId, action, sig, strength));
        }
    }
}
