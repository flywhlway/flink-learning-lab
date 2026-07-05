package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e03-C4 · AggregatingState:每用户客单价滚动平均。
 *
 * <p>与"ValueState 手搓 sum/cnt"等价,但聚合逻辑内聚在 AggregateFunction 里,
 * add 即写、get 即读,状态里只落累加器 —— ReducingState 是它 IN=OUT 的特例。
 */
public final class C4AggregatingStateAvgJob {
    private C4AggregatingStateAvgJob() {
    }

    /** 累加器 POJO(独立可序列化)。 */
    public static class AvgAcc {
        public double sum;
        public long cnt;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "orders", 20, 5, 6, 500)
            .keyBy(e -> e.userId)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private transient AggregatingState<Event, Double> avg;

                @Override
                public void open(OpenContext ctx) {
                    avg = getRuntimeContext().getAggregatingState(
                            new AggregatingStateDescriptor<>("avg-amount",
                                    new AggregateFunction<Event, AvgAcc, Double>() {
                                        @Override public AvgAcc createAccumulator() {
                                            return new AvgAcc();
                                        }
                                        @Override public AvgAcc add(Event e, AvgAcc a) {
                                            a.sum += e.amount; a.cnt++; return a;
                                        }
                                        @Override public Double getResult(AvgAcc a) {
                                            return a.cnt == 0 ? 0 : a.sum / a.cnt;
                                        }
                                        @Override public AvgAcc merge(AvgAcc x, AvgAcc y) {
                                            x.sum += y.sum; x.cnt += y.cnt; return x;
                                        }
                                    },
                                    AvgAcc.class));
                }

                @Override
                public void processElement(Event e, Context ctx, Collector<String> out)
                        throws Exception {
                    avg.add(e);
                    out.collect("user=%s avg=%.2f (this=%.1f)".formatted(
                            e.userId, avg.get(), e.amount));
                }
            })
            .uid("e03-c4-avg")
            .print();

        env.execute("e03-c4-aggregating-state-avg");
    }
}
