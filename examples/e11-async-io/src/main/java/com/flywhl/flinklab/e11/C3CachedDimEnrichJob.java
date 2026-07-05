package com.flywhl.flinklab.e11;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.api.common.functions.OpenContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * e11-C3 · 两级维表:本地 LRU 缓存(L1)+ 异步远端(L2)。
 *
 * <p>维表富化的生产标配:热 key 命中本地缓存(纳秒级、零外呼),未命中才异步查远端并回填。
 * 输出带 HIT/MISS 标记,长跑可见命中率爬升。三个工程要点:
 * ① 缓存是 subtask 本地的普通对象,不是 Flink 状态(重启即失,可接受——它只是加速器);
 * ② 容量上限 + LRU 淘汰防内存失控;③ 缓存 TTL 决定"维度更新可见延迟",要与业务对齐
 * (SQL 世界的等价物是 Lookup Join 的 cache 参数,e07-C2)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e11-async-io \
 *          -Dexec.mainClass=com.flywhl.flinklab.e11.C3CachedDimEnrichJob
 */
public final class C3CachedDimEnrichJob {
    private C3CachedDimEnrichJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        AsyncDataStream.unorderedWait(
                        Labs.events(env, "clicks", 50, 8, 10, 500),
                        new CachedEnrich(100), 3, TimeUnit.SECONDS, 100)
                .uid("e11-c3-cached")
                .print();

        env.execute("e11-c3-cached-dim-enrich");
    }

    /** L1=LRU Map,L2=FakeDimClient。 */
    public static final class CachedEnrich extends RichAsyncFunction<Event, String> {
        private final int capacity;
        private transient Map<String, String> lru;
        private transient FakeDimClient client;
        private transient ExecutorService pool;
        private transient long hits;
        private transient long total;

        public CachedEnrich(int capacity) {
            this.capacity = capacity;
        }

        @Override
        public void open(OpenContext ctx) {
            lru = new LinkedHashMap<>(capacity, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > capacity;
                }
            };
            client = new FakeDimClient(60, 0);
            pool = Executors.newFixedThreadPool(8);
        }

        @Override
        public void asyncInvoke(Event e, ResultFuture<String> rf) {
            total++;
            String cached = lru.get(e.userId);
            if (cached != null) {
                hits++;
                rf.complete(Collections.singleton(
                        "HIT  user=%s %s 命中率=%.0f%%".formatted(
                                e.userId, cached, 100.0 * hits / total)));
                return;
            }
            client.lookup(e.userId, pool).whenComplete((p, err) -> {
                if (err != null) {
                    rf.completeExceptionally(err);
                } else {
                    lru.put(e.userId, p);   // 回填(asyncInvoke 与回调同为 mailbox 协调,教学取舍见 README)
                    rf.complete(Collections.singleton("MISS user=%s %s(已回填)".formatted(e.userId, p)));
                }
            });
        }

        @Override
        public void close() {
            if (pool != null) {
                pool.shutdown();
            }
        }
    }
}
