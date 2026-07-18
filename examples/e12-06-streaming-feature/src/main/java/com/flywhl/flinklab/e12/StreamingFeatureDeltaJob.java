package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** e12-06 第二 Job · 特征增量：输出 amount 相对上次的 delta，供在线特征调试。 */
public final class StreamingFeatureDeltaJob {
    private StreamingFeatureDeltaJob() {}

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "feat-delta", 20, 4, 6, 200)
            .keyBy(e -> e.userId)
            .process(new DeltaFn())
            .uid("e12-06-feature-delta")
            .print();
        env.execute("e12-06-streaming-feature-delta");
    }

    public static final class DeltaFn extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Double> last;

        @Override
        public void open(OpenContext ctx) {
            last = getRuntimeContext().getState(new ValueStateDescriptor<>("last", Double.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            Double prev = last.value();
            double delta = prev == null ? 0.0 : e.amount - prev;
            last.update(e.amount);
            out.collect("DELTA user=%s amount=%.1f delta=%.1f".formatted(e.userId, e.amount, delta));
        }
    }
}
