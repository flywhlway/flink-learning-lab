package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ai/第01章 Demo · 轮询式 vs 事件驱动的异常响应延迟对照实验。
 *
 * <p>同一批"异常事件"(amount 突增)分别喂给两种检测逻辑:
 * ① 轮询分支:每 2 秒才检查一次"自上次以来是否出现过异常"(模拟定时批量扫描);
 * ② 事件驱动分支:异常事件一到即刻检测(Flink 算子的默认工作方式)。
 * 两分支各自记录"异常发生时间 → 被检测到时间"的延迟并打印对照。
 *
 * <p>预期:事件驱动分支延迟恒为个位数毫秒;轮询分支延迟均匀分布在
 * [0, 2000]ms 之间(取决于异常恰好发生在轮询周期的哪个位置)——
 * 这就是"延迟与轮询间隔正相关"的直观证据。把 POLL_INTERVAL_MS 调小能降低
 * 轮询分支延迟,但对应的是更频繁的全量/增量扫描负载,ai/01 第 5 节已论证
 * 这条路走到底就是自己拖垮自己。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-01-polling-vs-event \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.PollingVsEventDrivenJob
 */
public final class PollingVsEventDrivenJob {
    private PollingVsEventDrivenJob() {
    }

    private static final long POLL_INTERVAL_MS = 2_000;
    private static final double ANOMALY_THRESHOLD = 480;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = Labs.events(env, "signals", 30, 5, 8, 200);

        // 事件驱动分支:数据一到就判断,处理耗时即"检测延迟"
        events.process(new EventDrivenDetector()).uid("e12-01-event-driven").print();

        // 轮询分支:用处理时间定时器模拟"每 POLL_INTERVAL_MS 才检查一次"
        events.keyBy(e -> e.userId)
              .process(new PollingSimulatorDetector(POLL_INTERVAL_MS))
              .uid("e12-01-polling")
              .print();

        env.execute("e12-01-polling-vs-event-driven");
    }

    /** 事件驱动:异常事件到达的瞬间即输出检测结果,延迟只含算子处理耗时。 */
    public static final class EventDrivenDetector
            extends org.apache.flink.streaming.api.functions.ProcessFunction<Event, String> {
        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) {
            if (e.amount > ANOMALY_THRESHOLD) {
                long detectedAt = System.currentTimeMillis();
                out.collect("EVENT-DRIVEN  user=%s 发生于=%d 检测于=%d 延迟=%dms"
                        .formatted(e.userId, e.ts, detectedAt, detectedAt - e.ts));
            }
        }
    }

    /**
     * 轮询模拟:不在异常发生时立即检测,而是缓存"最近一次异常",
     * 由周期性处理时间定时器统一"翻牌"检测——这就是批量轮询的行为模式。
     */
    public static final class PollingSimulatorDetector
            extends KeyedProcessFunction<String, Event, String> {
        private final long intervalMs;
        private transient ValueState<Event> pendingAnomaly;
        private transient ValueState<Long> nextPoll;

        public PollingSimulatorDetector(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        @Override
        public void open(OpenContext ctx) {
            pendingAnomaly = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pending-anomaly", Event.class));
            nextPoll = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("next-poll", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            if (e.amount > ANOMALY_THRESHOLD) {
                pendingAnomaly.update(e);   // 异常发生,但不立即处理——等下一次"轮询"
            }
            if (nextPoll.value() == null) {
                long t = ctx.timerService().currentProcessingTime() + intervalMs;
                nextPoll.update(t);
                ctx.timerService().registerProcessingTimeTimer(t);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            Event anomaly = pendingAnomaly.value();
            if (anomaly != null) {
                long detectedAt = System.currentTimeMillis();
                out.collect("POLLING       user=%s 发生于=%d 检测于=%d 延迟=%dms(轮询周期=%dms)"
                        .formatted(anomaly.userId, anomaly.ts, detectedAt,
                                detectedAt - anomaly.ts, intervalMs));
                pendingAnomaly.clear();
            }
            long t = timestamp + intervalMs;
            nextPoll.update(t);
            ctx.timerService().registerProcessingTimeTimer(t);
        }
    }
}
