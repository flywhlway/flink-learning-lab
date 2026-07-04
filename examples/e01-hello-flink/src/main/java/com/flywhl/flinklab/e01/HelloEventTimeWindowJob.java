package com.flywhl.flinklab.e01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * e01-J1 · 事件时间 Tumbling Window 最小闭环(本地运行,零外部依赖)。
 *
 * <p>验证点:窗口归属由 <b>事件时间</b> 决定,与元素到达顺序无关 ——
 * 输入序列刻意乱序,但每个 10s 窗口的计数结果与"按时间轴排好序"的结果完全一致。
 *
 * <p>运行(仓库根目录):
 * <pre>
 * cd examples
 * mvn -q -Plocal compile exec:java -pl e01-hello-flink \
 *     -Dexec.mainClass=com.flywhl.flinklab.e01.HelloEventTimeWindowJob
 * </pre>
 *
 * <p>注意:有界输入在结束时会发出 MAX_WATERMARK,因此所有窗口在作业收尾统一触发;
 * 迟到数据 / allowedLateness 的行为必须在无界流上才能真实观察,见 e02 模块。
 */
public final class HelloEventTimeWindowJob {

    private HelloEventTimeWindowJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 演示可读性优先;并行度对窗口语义无影响,对输出顺序有影响

        long base = Instant.parse("2026-07-04T00:00:00Z").toEpochMilli();

        // 乱序输入:第 3 条(+2s)在 +12s 之后才到达
        DataStream<ClickEvent> clicks = env.fromData(
                new ClickEvent("u1", "/home",  base + 1_000),
                new ClickEvent("u2", "/home",  base + 3_000),
                new ClickEvent("u1", "/cart",  base + 12_000),
                new ClickEvent("u3", "/home",  base + 2_000),   // 乱序:属于第 1 个窗口
                new ClickEvent("u2", "/cart",  base + 15_000),
                new ClickEvent("u1", "/pay",   base + 21_000),
                new ClickEvent("u3", "/cart",  base + 13_000)); // 乱序:属于第 2 个窗口

        clicks
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, recordTs) -> e.ts))
                .keyBy(e -> e.page)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new CountAgg(), new AttachWindow())
                .name("page-clicks-10s")
                .uid("e01-j1-window")
                .print();

        env.execute("e01-hello-event-time-window");
    }

    /** 增量聚合:每来一条 +1,窗口状态只存一个 Long(而非缓存全部元素)。 */
    static final class CountAgg implements AggregateFunction<ClickEvent, Long, Long> {
        @Override public Long createAccumulator()                { return 0L; }
        @Override public Long add(ClickEvent v, Long acc)        { return acc + 1; }
        @Override public Long getResult(Long acc)                { return acc; }
        @Override public Long merge(Long a, Long b)              { return a + b; }
    }

    /** 全窗口函数只负责"贴窗口元信息",与增量聚合组合是生产标准写法。 */
    static final class AttachWindow
            extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String page, Context ctx, Iterable<Long> counts, Collector<String> out) {
            long c = counts.iterator().next();
            out.collect("window[%s ~ %s] page=%s clicks=%d".formatted(
                    Instant.ofEpochMilli(ctx.window().getStart()),
                    Instant.ofEpochMilli(ctx.window().getEnd()),
                    page, c));
        }
    }
}
