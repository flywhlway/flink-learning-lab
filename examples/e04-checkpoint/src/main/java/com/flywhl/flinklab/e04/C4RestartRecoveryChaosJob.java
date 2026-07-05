package com.flywhl.flinklab.e04;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * e04-C4 · 混沌实验:注入故障 → 指数退避重启 → 状态从 checkpoint 恢复。
 *
 * <p>故障注入:attempt&lt;2 时,处理满 200 条即抛异常。观察输出:
 * attempt=0 计数涨到 ~200 → 崩;attempt=1 从**最近 checkpoint 的计数**续跑
 * (不是从 0,也不是精确从 200 —— checkpoint 之后、崩溃之前的那段被回放,
 * 这就是"作业内 exactly-once ≠ 输出端 exactly-once":print 端能看到少量重复行,
 * 正是 e04-C2 里 Kafka 端到端事务要解决的问题);attempt=2 起不再注入,长跑稳定。
 *
 * <p>WebUI http://localhost:8083 → 作业 Overview 可看到 restart 次数与
 * exponential-delay 的退避间隔(1s→2s→…)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e04-checkpoint \
 *          -Dexec.mainClass=com.flywhl.flinklab.e04.C4RestartRecoveryChaosJob
 */
public final class C4RestartRecoveryChaosJob {
    private C4RestartRecoveryChaosJob() {
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = Configuration.fromMap(Map.of(
                "rest.port", "8083",
                "execution.checkpointing.dir", "file:///tmp/flink-lab/e04-c4",
                "restart-strategy.type", "exponential-delay",
                "restart-strategy.exponential-delay.initial-backoff", "1s",
                "restart-strategy.exponential-delay.max-backoff", "10s"));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.enableCheckpointing(5_000);

        Labs.events(env, "chaos", 100, 5, 10, 500)
            .keyBy(e -> e.userId)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private transient ValueState<Long> cnt;
                private transient long localSeen; // 非状态计数:仅本 attempt 生命周期内有效

                @Override
                public void open(OpenContext ctx) {
                    cnt = getRuntimeContext().getState(
                            new ValueStateDescriptor<>("cnt", Long.class));
                    localSeen = 0;
                }

                @Override
                public void processElement(Event e, Context ctx, Collector<String> out)
                        throws Exception {
                    long c = (cnt.value() == null ? 0 : cnt.value()) + 1;
                    cnt.update(c);
                    localSeen++;

                    int attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();
                    if (attempt < 2 && localSeen >= 200) {
                        throw new RuntimeException(
                                "注入故障 attempt=" + attempt + "(实验预期,非 bug)");
                    }
                    if (c % 50 == 0) {
                        out.collect("attempt=%d user=%s state-cnt=%d(状态延续) local=%d(每次归零)"
                                .formatted(attempt, e.userId, c, localSeen));
                    }
                }
            })
            .uid("e04-c4-chaos")
            .print();

        env.execute("e04-c4-restart-recovery-chaos");
    }
}
