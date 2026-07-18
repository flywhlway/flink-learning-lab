package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ai/第23章 Demo · 在线学习样本管线：特征样本侧输出（不真训 Ray）。
 *
 * <p>主流做实时打分日志；当 amount 变化显著时，把 (features, label) 样本
 * 写入 Side Output，模拟交给训练集群的数据通道。Flink 负责样本正确性与吞吐，
 * 训练留给 Ray/批作业。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-23-online-learning-sample \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.OnlineLearningSampleJob
 */
public final class OnlineLearningSampleJob {
    private OnlineLearningSampleJob() {
    }

    public static final OutputTag<String> TRAIN_SAMPLE =
            new OutputTag<String>("train-sample") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> scored = Labs.events(env, "ol", 20, 3, 8, 200)
                .keyBy(e -> e.userId)
                .process(new SampleEmitter())
                .uid("e12-23-sample");

        scored.print("SCORE");
        scored.getSideOutput(TRAIN_SAMPLE).print("SAMPLE");

        env.execute("e12-23-online-learning-sample");
    }

    public static final class SampleEmitter extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Double> lastAmount;

        @Override
        public void open(OpenContext ctx) {
            lastAmount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-amount", Double.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            Double prev = lastAmount.value();
            double score = e.amount / 500.0;
            out.collect("SCORE  user=%s page=%s score=%.3f".formatted(e.userId, e.page, score));
            if (prev != null && Math.abs(e.amount - prev) > 120) {
                int label = e.amount > prev ? 1 : 0;
                ctx.output(TRAIN_SAMPLE,
                        "SAMPLE user=%s features=page:%s,prev:%.1f,cur:%.1f label=%d"
                                .formatted(e.userId, e.page, prev, e.amount, label));
            }
            lastAmount.update(e.amount);
        }
    }
}
