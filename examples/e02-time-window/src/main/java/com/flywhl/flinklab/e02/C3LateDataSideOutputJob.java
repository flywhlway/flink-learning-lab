package com.flywhl.flinklab.e02;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;

/**
 * e02-C3 · 迟到数据的三层命运:
 * ① watermark(2s)内 → 正常入窗;
 * ② 超 watermark 但在 allowedLateness(8s)内 → 窗口**重新触发**(同窗口多次输出,下游必须幂等!);
 * ③ 超过 allowedLateness → 进 side output(生产上接死信 topic 做对账)。
 *
 * <p>数据乱序上界拉到 12s,保证三种情况都出现。观察输出前缀 MAIN / LATE。
 * MAIN 中同一 window+page 出现多次且 count 递增,就是"迟到重触发"的样子。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e02-time-window \
 *          -Dexec.mainClass=com.flywhl.flinklab.e02.C3LateDataSideOutputJob
 */
public final class C3LateDataSideOutputJob {
    private C3LateDataSideOutputJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 匿名子类:保留泛型信息,OutputTag 的固定写法
        final OutputTag<Event> tooLate = new OutputTag<>("too-late") {
        };

        SingleOutputStreamOperator<String> main =
            Labs.events(env, "clicks", 40, 5, 10, 12_000)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(2)))
                .keyBy(e -> e.page)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .allowedLateness(Duration.ofSeconds(8))
                .sideOutputLateData(tooLate)
                .aggregate(new com.flywhl.flinklab.e02.support.CountAgg(),
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String page, Context ctx,
                                                Iterable<Long> in, Collector<String> out) {
                                out.collect("MAIN window[%s] page=%s count=%d wm=%s".formatted(
                                        Instant.ofEpochMilli(ctx.window().getStart()), page,
                                        in.iterator().next(),
                                        Instant.ofEpochMilli(ctx.currentWatermark())));
                            }
                        })
                .uid("e02-c3-window");

        main.print();
        main.getSideOutput(tooLate)
            .map(e -> "LATE(dropped from window) " + e)
            .print();

        env.execute("e02-c3-late-data-side-output");
    }
}
