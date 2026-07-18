package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** e12-18 第二 Job · 软封顶：超预算降级为短回复标记，而非硬熔断。 */
public final class StreamingCostSoftCapJob {
    private StreamingCostSoftCapJob() {}
    private static final double BUDGET = 2000.0;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "soft-cap", 20, 3, 8, 150)
            .keyBy(e -> e.userId)
            .process(new SoftCap())
            .uid("e12-18-soft-cap")
            .print();
        env.execute("e12-18-streaming-cost-soft-cap");
    }

    public static final class SoftCap extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Double> spent;

        @Override
        public void open(OpenContext ctx) {
            spent = getRuntimeContext().getState(new ValueStateDescriptor<>("spent", Double.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            double s = (spent.value() == null ? 0.0 : spent.value()) + e.amount;
            spent.update(s);
            if (s > BUDGET) {
                out.collect("SOFT-CAP user=%s spent=%.1f mode=short".formatted(e.userId, s));
            } else {
                out.collect("OK user=%s spent=%.1f mode=full".formatted(e.userId, s));
            }
        }
    }
}
