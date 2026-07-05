package com.flywhl.flinklab.e02;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * e02-C4 · 自定义 Trigger:大窗口 + 提前输出(大屏/看板的标准需求)。
 *
 * <p>1 分钟事件时间窗口,业务要求"每 10 秒刷新一次当前值" —— 用处理时间定时器
 * 周期性 FIRE(不 PURGE,累计值继续增长),窗口结束时 FIRE 收尾,clear() 清理定时器与状态。
 *
 * <p>关键语义:FIRE 输出的是**同一窗口的中间态**,下游按 (window, key) 幂等覆盖;
 * 若误用 FIRE_AND_PURGE,输出会变成增量而非累计 —— 两种都对,但必须与下游约定一致。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e02-time-window \
 *          -Dexec.mainClass=com.flywhl.flinklab.e02.C4EarlyFireTriggerJob
 */
public final class C4EarlyFireTriggerJob {
    private C4EarlyFireTriggerJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "clicks", 50, 5, 10, 2_000)
            .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(2)))
            .keyBy(e -> e.page)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .trigger(new PeriodicEarlyFireTrigger(10_000))
            .aggregate(new com.flywhl.flinklab.e02.support.CountAgg(),
                    new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                        @Override
                        public void process(String page, Context ctx,
                                            Iterable<Long> in, Collector<String> out) {
                            out.collect("window[%s] page=%s running-count=%d".formatted(
                                    Instant.ofEpochMilli(ctx.window().getStart()),
                                    page, in.iterator().next()));
                        }
                    })
            .uid("e02-c4-early-fire")
            .print();

        env.execute("e02-c4-early-fire-trigger");
    }

    /** 处理时间周期提前触发 + 事件时间收尾的组合 Trigger。 */
    public static final class PeriodicEarlyFireTrigger extends Trigger<Object, TimeWindow> {

        private final long intervalMs;
        private final ValueStateDescriptor<Long> nextFireDesc =
                new ValueStateDescriptor<>("next-fire", Long.class);

        public PeriodicEarlyFireTrigger(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        @Override
        public TriggerResult onElement(Object element, long timestamp,
                                       TimeWindow window, TriggerContext ctx) throws Exception {
            // 事件时间收尾定时器(窗口自身的"最终触发")
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // 首条元素时安排第一个周期定时器
            ValueState<Long> nextFire = ctx.getPartitionedState(nextFireDesc);
            if (nextFire.value() == null) {
                long t = ctx.getCurrentProcessingTime() + intervalMs;
                nextFire.update(t);
                ctx.registerProcessingTimeTimer(t);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window,
                                              TriggerContext ctx) throws Exception {
            ValueState<Long> nextFire = ctx.getPartitionedState(nextFireDesc);
            long t = time + intervalMs;
            nextFire.update(t);
            ctx.registerProcessingTimeTimer(t);
            return TriggerResult.FIRE;              // 中间态输出,不清窗口状态
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return time == window.maxTimestamp()
                    ? TriggerResult.FIRE_AND_PURGE   // 终局输出并释放窗口状态
                    : TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Long> nextFire = ctx.getPartitionedState(nextFireDesc);
            if (nextFire.value() != null) {
                ctx.deleteProcessingTimeTimer(nextFire.value());
                nextFire.clear();
            }
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }
    }
}
