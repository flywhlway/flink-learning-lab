package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * e03-C10 · RocksDB 后端 + 增量 checkpoint 的本地观察实验。
 *
 * <p>用 Configuration 切到 rocksdb 后端(与生产配置同一套键),checkpoint 落
 * file:///tmp/flink-lab/e03-ckpt。跑起来后另开终端:
 * <pre>
 *   watch -n2 'du -sh /tmp/flink-lab/e03-ckpt/*/chk-* 2>/dev/null | tail -5'
 * </pre>
 * 观察:开增量后,稳态下每个 chk-N 目录只含新增/变更的 SST 引用,体积远小于全量;
 * shared/ 目录才是大头(被多个 checkpoint 共享的 SST)—— 这就是"删旧 checkpoint
 * 不一定省空间"的原因(面试题 11)。
 *
 * <p>状态构造:每 key 维护一个持续增长的 MapState,制造"值得增量"的状态规模。
 */
public final class C10RocksDbBackendLabJob {
    private C10RocksDbBackendLabJob() {
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = Configuration.fromMap(Map.of(
                "state.backend.type", "rocksdb",
                "execution.checkpointing.incremental", "true",
                "execution.checkpointing.dir", "file:///tmp/flink-lab/e03-ckpt"));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(10_000);

        Labs.events(env, "grow", 200, 50, 10, 500)
            .keyBy(e -> e.userId)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private transient MapState<Long, String> history;

                @Override
                public void open(OpenContext ctx) {
                    history = getRuntimeContext().getMapState(
                            new MapStateDescriptor<>("history", Long.class, String.class));
                }

                @Override
                public void processElement(Event e, Context ctx, Collector<String> out)
                        throws Exception {
                    history.put(e.ts, e.page);   // 只增不删,状态持续膨胀(实验目的)
                    if (e.ts % 97 == 0) {
                        out.collect("state growing... user=" + e.userId);
                    }
                }
            })
            .uid("e03-c10-rocksdb")
            .print();

        env.execute("e03-c10-rocksdb-incremental-lab");
    }
}
