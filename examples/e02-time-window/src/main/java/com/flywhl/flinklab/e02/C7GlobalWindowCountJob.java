package com.flywhl.flinklab.e02;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e02-C7 · 计数触发的「伪窗口」：每 N 条用定时器刷出批次，对照时间窗口。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e02-time-window \
 *          -Dexec.mainClass=com.flywhl.flinklab.e02.C7GlobalWindowCountJob
 */
public final class C7GlobalWindowCountJob {
    private C7GlobalWindowCountJob() {
    }

    private static final int BATCH = 5;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "count-win", 25, 3, 8, 150)
            .keyBy(e -> e.userId)
            .process(new CountBatch())
            .uid("e02-c7-count-batch")
            .print();
        env.execute("e02-c7-count-batch");
    }

    public static final class CountBatch extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Integer> cnt;
        private transient ValueState<Double> sum;

        @Override
        public void open(OpenContext ctx) {
            cnt = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Integer.class));
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Double.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            int c = (cnt.value() == null ? 0 : cnt.value()) + 1;
            double s = (sum.value() == null ? 0.0 : sum.value()) + e.amount;
            if (c >= BATCH) {
                out.collect("BATCH user=%s size=%d sum=%.1f".formatted(e.userId, c, s));
                cnt.clear();
                sum.clear();
            } else {
                cnt.update(c);
                sum.update(s);
            }
        }
    }
}
