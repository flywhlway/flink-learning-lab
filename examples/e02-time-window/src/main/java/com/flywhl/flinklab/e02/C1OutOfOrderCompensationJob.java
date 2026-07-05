package com.flywhl.flinklab.e02;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * e02-C1 · 乱序补偿实验:watermark 上界(0s vs 5s)对同一数据的窗口结果影响。
 *
 * <p>数据乱序 ≤4s。预期:bound=5s 的分支每个窗口计数更高且两分支同窗口计数差
 * 即"被 0s 分支丢弃的迟到数据量" —— 乱序上界是「延迟换正确性」的旋钮,不是玄学参数。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e02-time-window \
 *          -Dexec.mainClass=com.flywhl.flinklab.e02.C1OutOfOrderCompensationJob
 */
public final class C1OutOfOrderCompensationJob {
    private C1OutOfOrderCompensationJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> raw = Labs.events(env, "clicks", 50, 5, 10, 4_000);

        attach(raw, Duration.ZERO,          "bound=0s");
        attach(raw, Duration.ofSeconds(5),  "bound=5s");

        env.execute("e02-c1-out-of-order-compensation");
    }

    private static void attach(DataStream<Event> raw, Duration bound, String tag) {
        raw.assignTimestampsAndWatermarks(Labs.boundedWm(bound))
           .keyBy(e -> e.page)
           .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
           .aggregate(new com.flywhl.flinklab.e02.support.CountAgg(),
                   new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                       @Override
                       public void process(String page, Context ctx,
                                           Iterable<Long> in, Collector<String> out) {
                           out.collect("[%s] window[%s] page=%s count=%d".formatted(
                                   tag, Instant.ofEpochMilli(ctx.window().getStart()),
                                   page, in.iterator().next()));
                       }
                   })
           .uid("e02-c1-" + tag)
           .print();
    }
}
