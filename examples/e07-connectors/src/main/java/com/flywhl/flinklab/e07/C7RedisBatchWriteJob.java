package com.flywhl.flinklab.e07;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * e07-C7 · Redis 攒批写:jedis Pipeline + CheckpointedFunction(e03-C5 模式的生产复刻)。
 *
 * <p>与 e03-C5 的区别只在"flush 动作"——那里是打印,这里是
 * jedis Pipeline 批量 SET(比逐条 SET 少 N-1 次网络往返)。
 * 容错点完全一致:未 flush 的尾巴随 snapshotState 存进 Operator ListState,
 * 故障恢复后从 initializeState 续上,不丢批次内数据(但 Redis 端是 at-least-once,
 * 重复 SET 同 key 幂等无害;若是 INCR 类操作需额外去重设计)。
 *
 * <p>提交(集群,需 docker/ 中已起 redis 服务):flink run -d -c 本类 e07-connectors jar;
 * 验证:redis-cli KEYS 'profile:*' 数量随时间增长。
 */
public final class C7RedisBatchWriteJob {
    private C7RedisBatchWriteJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        Labs.events(env, "orders", 50, 20, 8, 500)
            .map(new RedisBatchWriter(20))
            .uid("e07-c7-redis")
            .print();

        env.execute("e07-c7-redis-batch-write");
    }

    /** 攒批 + jedis pipeline flush + Operator State 容错(骨架同 e03-C5)。 */
    public static final class RedisBatchWriter
            extends RichMapFunction<Event, String>
            implements CheckpointedFunction {

        private final int threshold;
        private transient List<Event> buffer;
        private transient ListState<Event> checkpointed;
        private transient Jedis jedis;

        public RedisBatchWriter(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(OpenContext ctx) {
            buffer = new ArrayList<>();
            jedis = new Jedis("redis", 6379);
        }

        @Override
        public String map(Event e) {
            buffer.add(e);
            if (buffer.size() >= threshold) {
                return flushToRedis();
            }
            return "buffering(%d/%d)".formatted(buffer.size(), threshold);
        }

        private String flushToRedis() {
            Pipeline p = jedis.pipelined();
            for (Event e : buffer) {
                p.set("profile:" + e.userId, "%s@%d".formatted(e.page, e.ts));
            }
            p.sync();
            int n = buffer.size();
            buffer.clear();
            return "FLUSH batch=%d → redis pipeline".formatted(n);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            checkpointed.update(buffer);
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            checkpointed = ctx.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("redis-pending", Event.class));
            if (ctx.isRestored()) {
                for (Event e : checkpointed.get()) {
                    buffer.add(e);
                }
            }
        }

        @Override
        public void close() {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
