package com.flywhl.flinklab.e04;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e04-C3(V1) · Savepoint 升级实验的"旧版本"作业:每用户累计事件数。
 *
 * <p>实验完整流程(集群):
 * <pre>
 * # 1) 提交 V1,跑 1~2 分钟让 total 涨起来
 * docker compose exec jobmanager flink run -d \
 *     -c com.flywhl.flinklab.e04.C3SavepointJobV1 /opt/flink/usrlib/e04-checkpoint-0.1.0.jar
 * # 2) 带 savepoint 优雅停止(记下返回的 savepoint 路径)
 * docker compose exec jobmanager flink stop --savepointPath s3://flink/savepoints <jobId>
 * # 3) 用 V2 从 savepoint 恢复 —— 输出格式变了,但 total **续着涨**(状态按 uid 对上了)
 * docker compose exec jobmanager flink run -d -s <savepoint路径> \
 *     -c com.flywhl.flinklab.e04.C3SavepointJobV2 /opt/flink/usrlib/e04-checkpoint-0.1.0.jar
 * </pre>
 * V2 相对 V1:输出格式升级 + 新增一个无状态算子;有状态算子 uid 保持 "e04-c3-count"
 * 不变 —— 这就是升级不丢状态的全部秘密。反向实验:把 V2 的 uid 改掉重跑,
 * 恢复即失败(需 --allowNonRestoredState,且状态归零)。
 */
public final class C3SavepointJobV1 {
    private C3SavepointJobV1() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        Labs.events(env, "traffic", 50, 8, 8, 500)
            .keyBy(e -> e.userId)
            .process(new CountFn())
            .uid("e04-c3-count")            // ← 升级契约:V1/V2 必须一致
            .print();

        env.execute("e04-c3-savepoint-v1");
    }

    /** V1 逻辑:纯累计。 */
    public static class CountFn extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<Long> total;

        @Override
        public void open(OpenContext ctx) {
            total = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("total", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out)
                throws Exception {
            long t = (total.value() == null ? 0 : total.value()) + 1;
            total.update(t);
            out.collect("v1 user=%s total=%d".formatted(e.userId, t));
        }
    }
}
