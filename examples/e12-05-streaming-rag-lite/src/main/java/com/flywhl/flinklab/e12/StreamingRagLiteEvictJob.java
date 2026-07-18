package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/** e12-05 第二 Job · 处理时间定时淘汰过期片段，对照主 Job 的容量上限策略。 */
public final class StreamingRagLiteEvictJob {
    private StreamingRagLiteEvictJob() {}
    private static final long EVICT_MS = 4_000;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "rag-evict", 12, 3, 6, 200)
            .keyBy(e -> e.userId)
            .process(new EvictIndex())
            .uid("e12-05-rag-evict")
            .print();
        env.execute("e12-05-streaming-rag-lite-evict");
    }

    public static final class EvictIndex extends KeyedProcessFunction<String, Event, String> {
        private transient ListState<String> chunks;
        private transient org.apache.flink.api.common.state.ValueState<Long> timer;

        @Override
        public void open(OpenContext ctx) {
            chunks = getRuntimeContext().getListState(new ListStateDescriptor<>("chunks", String.class));
            timer = getRuntimeContext().getState(
                    new org.apache.flink.api.common.state.ValueStateDescriptor<>("t", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            chunks.add("%s@%d".formatted(e.page, e.ts));
            out.collect("INDEX user=%s add=%s".formatted(e.userId, e.page));
            Long t = timer.value();
            if (t == null) {
                long fire = ctx.timerService().currentProcessingTime() + EVICT_MS;
                timer.update(fire);
                ctx.timerService().registerProcessingTimeTimer(fire);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<String> left = new ArrayList<>();
            for (String c : chunks.get()) left.add(c);
            chunks.clear();
            out.collect("EVICT user=%s cleared=%d".formatted(ctx.getCurrentKey(), left.size()));
            timer.clear();
        }
    }
}
