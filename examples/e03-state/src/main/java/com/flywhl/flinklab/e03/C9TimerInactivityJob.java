package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * e03-C9 · Timer:用户不活跃(10s)超时检测 —— ProcessFunction 的看家本领。
 *
 * <p>套路:每来一条事件,①删旧定时器 ②注册 lastSeen+10s 的事件时间定时器;
 * onTimer 触发即"该用户静默满 10s"。定时器随状态一起 checkpoint,故障恢复后照常触发。
 *
 * <p>注意:事件时间定时器靠 watermark 驱动 —— 单 key 沉默没关系(其他 key 的
 * 事件推着 watermark 走),**全流沉默**时 watermark 停摆、定时器永不触发,
 * 兜底要靠处理时间定时器双保险(此处已注册,二触发先到先得)。
 */
public final class C9TimerInactivityJob {
    private C9TimerInactivityJob() {
    }

    private static final long GAP_MS = 10_000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "activity", 20, 4, 12, 500)
            .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)))
            .keyBy(e -> e.userId)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private transient ValueState<Long> timerTs;

                @Override
                public void open(OpenContext ctx) {
                    timerTs = getRuntimeContext().getState(
                            new ValueStateDescriptor<>("timer-ts", Long.class));
                }

                @Override
                public void processElement(Event e, Context ctx, Collector<String> out)
                        throws Exception {
                    Long old = timerTs.value();
                    if (old != null) {
                        ctx.timerService().deleteEventTimeTimer(old);
                    }
                    long fire = e.ts + GAP_MS;
                    ctx.timerService().registerEventTimeTimer(fire);
                    timerTs.update(fire);
                }

                @Override
                public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) {
                    out.collect("IDLE user=%s 静默至 %s(事件时间)".formatted(
                            ctx.getCurrentKey(), Instant.ofEpochMilli(ts)));
                }
            })
            .uid("e03-c9-inactivity")
            .print();

        env.execute("e03-c9-timer-inactivity");
    }
}
