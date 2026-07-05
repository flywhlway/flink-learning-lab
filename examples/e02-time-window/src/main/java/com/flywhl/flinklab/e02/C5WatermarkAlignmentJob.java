package com.flywhl.flinklab.e02;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * e02-C5 · 多流 watermark 实验:取小原则 + Watermark Alignment(FLIP-182)。
 *
 * <p>两个逻辑时钟源:fast 每条前进 1000ms,slow 每条前进 100ms(同样 20 eps,
 * 事件时间速度差 10 倍)。union 后窗口按 min(watermark) 推进 —— 输出里
 * wm 始终贴着 slow 的进度,而 fast 的数据在算子里越积越多(这就是生产中
 * "一个慢分区拖爆全作业状态"的机理)。
 *
 * <p>加对齐:两源同组、最大漂移 30s —— fast 源会被**暂停拉取**,把漂移压回 30s 内,
 * 状态积压随之封顶。对比方式:把 ALIGN 常量改为 false 再跑一遍,观察 drift 输出差异。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e02-time-window \
 *          -Dexec.mainClass=com.flywhl.flinklab.e02.C5WatermarkAlignmentJob
 */
public final class C5WatermarkAlignmentJob {
    private C5WatermarkAlignmentJob() {
    }

    private static final boolean ALIGN = true;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        long base = System.currentTimeMillis();

        DataStream<Event> fast = Labs.pacedEvents(env, "fast", 20, base, 1_000)
                .assignTimestampsAndWatermarks(strategy());
        DataStream<Event> slow = Labs.pacedEvents(env, "slow", 20, base, 100)
                .assignTimestampsAndWatermarks(strategy());

        fast.union(slow)
            .keyBy(e -> e.page)   // page 即源名(fast/slow)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .aggregate(new com.flywhl.flinklab.e02.support.CountAgg(),
                    new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                        @Override
                        public void process(String src, Context ctx,
                                            Iterable<Long> in, Collector<String> out) {
                            long driftSec = (System.currentTimeMillis()
                                    - ctx.currentWatermark()) / 1000;
                            out.collect("window[%s] src=%-4s count=%-3d 全局wm落后墙钟≈%ds".formatted(
                                    Instant.ofEpochMilli(ctx.window().getStart()),
                                    src, in.iterator().next(), driftSec));
                        }
                    })
            .uid("e02-c5-union-window")
            .print();

        env.execute("e02-c5-watermark-alignment");
    }

    private static WatermarkStrategy<Event> strategy() {
        WatermarkStrategy<Event> s = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((e, ts) -> e.ts);
        return ALIGN
                ? s.withWatermarkAlignment("lab-group", Duration.ofSeconds(30), Duration.ofSeconds(1))
                : s;
    }
}
