package com.flywhl.flinklab.e01;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e01-J5 · 按用户累计 min/max/avg(amount)：最小有状态算子，衔接 e03 状态专题。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e01-hello-flink \
 *          -Dexec.mainClass=com.flywhl.flinklab.e01.KeyedRunningStatsJob
 */
public final class KeyedRunningStatsJob {
    private KeyedRunningStatsJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "stats", 25, 4, 6, 200)
            .keyBy(e -> e.userId)
            .process(new RunningStats())
            .uid("e01-j5-stats")
            .print();
        env.execute("e01-keyed-running-stats");
    }

    public static final class RunningStats extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Double> min;
        private transient ValueState<Double> max;
        private transient ValueState<Double> sum;
        private transient ValueState<Long> n;

        @Override
        public void open(OpenContext ctx) {
            min = getRuntimeContext().getState(new ValueStateDescriptor<>("min", Double.class));
            max = getRuntimeContext().getState(new ValueStateDescriptor<>("max", Double.class));
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Double.class));
            n = getRuntimeContext().getState(new ValueStateDescriptor<>("n", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            double mi = min.value() == null ? e.amount : Math.min(min.value(), e.amount);
            double ma = max.value() == null ? e.amount : Math.max(max.value(), e.amount);
            double s = (sum.value() == null ? 0.0 : sum.value()) + e.amount;
            long c = (n.value() == null ? 0L : n.value()) + 1;
            min.update(mi);
            max.update(ma);
            sum.update(s);
            n.update(c);
            out.collect("STATS user=%s min=%.1f max=%.1f avg=%.1f n=%d"
                    .formatted(e.userId, mi, ma, s / c, c));
        }
    }
}
