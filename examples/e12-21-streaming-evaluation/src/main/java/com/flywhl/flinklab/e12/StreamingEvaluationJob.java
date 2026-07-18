package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ai/第21章 Demo · 流式评测：窗口化「准确率」与延迟模拟指标。
 *
 * <p>用 amount 奇偶模拟预测对错；窗口输出准确率与平均处理延迟。
 * 证明评测可以是流作业的一等公民，而不只是离线 notebook。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-21-streaming-evaluation \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingEvaluationJob
 */
public final class StreamingEvaluationJob {
    private StreamingEvaluationJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = Labs.events(env, "eval", 30, 4, 5, 400)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(500))
                                .withTimestampAssigner((e, ts) -> e.ts));

        events.keyBy(e -> e.userId)
              .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
              .aggregate(new EvalAgg(), new EvalReport())
              .uid("e12-21-eval-window")
              .print();

        env.execute("e12-21-streaming-evaluation");
    }

    /** acc = [correct, total, latencySum] */
    public static final class EvalAgg implements AggregateFunction<Event, long[], long[]> {
        @Override
        public long[] createAccumulator() {
            return new long[]{0, 0, 0};
        }

        @Override
        public long[] add(Event e, long[] acc) {
            boolean correct = ((long) e.amount) % 2 == 0; // 模拟标签匹配
            acc[0] += correct ? 1 : 0;
            acc[1] += 1;
            acc[2] += Math.max(0, System.currentTimeMillis() - e.ts);
            return acc;
        }

        @Override
        public long[] getResult(long[] acc) {
            return acc;
        }

        @Override
        public long[] merge(long[] a, long[] b) {
            return new long[]{a[0] + b[0], a[1] + b[1], a[2] + b[2]};
        }
    }

    public static final class EvalReport
            extends ProcessWindowFunction<long[], String, String, TimeWindow> {
        @Override
        public void process(String user, Context ctx, Iterable<long[]> values, Collector<String> out) {
            long[] v = values.iterator().next();
            double acc = v[1] == 0 ? 0.0 : (double) v[0] / v[1];
            double avgLat = v[1] == 0 ? 0.0 : (double) v[2] / v[1];
            out.collect("EVAL  user=%s window=[%d,%d) accuracy=%.2f avgLatencyMs=%.1f n=%d"
                    .formatted(user, ctx.window().getStart(), ctx.window().getEnd(),
                            acc, avgLat, v[1]));
        }
    }
}
