package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ai/第11章 Demo · 流内工作流：有限状态机 ProcessFunction。
 *
 * <p>每个用户会话走 NEW → SEEN → READY → DONE。用 page 名模拟工作流事件
 * (home=进入, cart=准备, pay=完成)。超时用处理时间定时器回退到 NEW，
 * 演示编排边界：复杂分支留给外呼工作流引擎，流内只保留可 checkpoint 的 FSM。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-11-streaming-workflow \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingWorkflowJob
 */
public final class StreamingWorkflowJob {
    private StreamingWorkflowJob() {
    }

    private static final long IDLE_MS = 5_000;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "wf", 15, 4, 8, 250)
            .keyBy(e -> e.userId)
            .process(new SessionFsm())
            .uid("e12-11-fsm")
            .print();

        env.execute("e12-11-streaming-workflow");
    }

    public static final class SessionFsm extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<String> state;
        private transient ValueState<Long> timerTs;

        @Override
        public void open(OpenContext ctx) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("wf-state", String.class));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<>("wf-timer", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            String s = state.value() == null ? "NEW" : state.value();
            String next = transition(s, e.page);
            if (!next.equals(s)) {
                out.collect("TRANS  user=%s %s -> %s (page=%s)".formatted(e.userId, s, next, e.page));
                state.update(next);
            } else {
                out.collect("STAY   user=%s state=%s page=%s".formatted(e.userId, s, e.page));
            }
            Long prev = timerTs.value();
            if (prev != null) {
                ctx.timerService().deleteProcessingTimeTimer(prev);
            }
            long t = ctx.timerService().currentProcessingTime() + IDLE_MS;
            timerTs.update(t);
            ctx.timerService().registerProcessingTimeTimer(t);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            String s = state.value();
            if (s != null && !"NEW".equals(s) && !"DONE".equals(s)) {
                out.collect("TIMEOUT user=%s %s -> NEW".formatted(ctx.getCurrentKey(), s));
                state.update("NEW");
            }
            timerTs.clear();
        }

        private static String transition(String s, String page) {
            return switch (s) {
                case "NEW" -> "home".equals(page) ? "SEEN" : s;
                case "SEEN" -> "cart".equals(page) ? "READY" : s;
                case "READY" -> "pay".equals(page) ? "DONE" : s;
                default -> s;
            };
        }
    }
}
