package com.flywhl.flinklab.e02;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * e02-C2 · 事件时间会话窗口:用户沉默超过 gap(5s)即切分一次会话。
 *
 * <p>数据源刻意做成突发式(每用户连发 15 条后轮空),因此每个用户会周期性产出
 * "会话结束"记录。观察点:会话窗口没有固定边界,窗口是**随数据合并出来的**——
 * 两条本不相邻的记录可能因为一条新纪录的到来被并成一个会话(MergingWindowAssigner)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e02-time-window \
 *          -Dexec.mainClass=com.flywhl.flinklab.e02.C2SessionWindowJob
 */
public final class C2SessionWindowJob {
    private C2SessionWindowJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "bursty-clicks", 30, 4, 15, 1_000)
            .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(2)))
            .keyBy(e -> e.userId)
            .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(5)))
            .aggregate(new com.flywhl.flinklab.e02.support.CountAgg(),
                    new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                        @Override
                        public void process(String user, Context ctx,
                                            Iterable<Long> in, Collector<String> out) {
                            long durSec = (ctx.window().getEnd() - ctx.window().getStart()) / 1000;
                            out.collect("session user=%s [%s ~ +%ds] events=%d".formatted(
                                    user, Instant.ofEpochMilli(ctx.window().getStart()),
                                    durSec, in.iterator().next()));
                        }
                    })
            .uid("e02-c2-session")
            .print();

        env.execute("e02-c2-session-window");
    }
}
