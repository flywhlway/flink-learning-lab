package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ai/第20章 Demo · Embedding 语义缓存：本地 LRU MapState + 命中率计数。
 *
 * <p>以 page 为缓存键，模拟 embedding 向量指纹(用 amount 哈希)。容量超限淘汰最旧键。
 * 输出 HIT/MISS 与运行命中率，对应 e11-C3 两级缓存的「本地层」教学。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-20-embedding-cache \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.EmbeddingCacheJob
 */
public final class EmbeddingCacheJob {
    private EmbeddingCacheJob() {
    }

    private static final int CAPACITY = 4;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "emb-cache", 20, 2, 10, 150)
            .keyBy(e -> e.userId)
            .process(new LruEmbeddingCache())
            .uid("e12-20-emb-cache")
            .print();

        env.execute("e12-20-embedding-cache");
    }

    public static final class LruEmbeddingCache extends KeyedProcessFunction<String, Event, String> {
        private transient MapState<String, Long> cache;
        private transient ValueState<Long> hits;
        private transient ValueState<Long> total;

        @Override
        public void open(OpenContext ctx) {
            cache = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("emb", String.class, Long.class));
            hits = getRuntimeContext().getState(new ValueStateDescriptor<>("hits", Long.class));
            total = getRuntimeContext().getState(new ValueStateDescriptor<>("total", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            long t = (total.value() == null ? 0L : total.value()) + 1;
            total.update(t);
            Long existing = cache.get(e.page);
            if (existing != null) {
                long h = (hits.value() == null ? 0L : hits.value()) + 1;
                hits.update(h);
                cache.put(e.page, e.ts); // 刷新 LRU 时间戳
                out.collect("HIT   user=%s page=%s hitRate=%.2f"
                        .formatted(e.userId, e.page, (double) h / t));
                return;
            }
            // MISS: 写入；超容量按最旧 ts 淘汰
            List<String> keys = new ArrayList<>();
            for (Iterator<String> it = cache.keys().iterator(); it.hasNext(); ) {
                keys.add(it.next());
            }
            if (keys.size() >= CAPACITY) {
                String victim = null;
                long oldest = Long.MAX_VALUE;
                for (String k : keys) {
                    Long ts = cache.get(k);
                    if (ts != null && ts < oldest) {
                        oldest = ts;
                        victim = k;
                    }
                }
                if (victim != null) {
                    cache.remove(victim);
                }
            }
            long fingerprint = Double.hashCode(e.amount);
            cache.put(e.page, e.ts);
            long h = hits.value() == null ? 0L : hits.value();
            out.collect("MISS  user=%s page=%s fp=%d hitRate=%.2f"
                    .formatted(e.userId, e.page, fingerprint, (double) h / t));
        }
    }
}
