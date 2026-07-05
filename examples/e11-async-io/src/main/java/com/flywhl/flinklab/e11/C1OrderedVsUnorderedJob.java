package com.flywhl.flinklab.e11;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.api.common.functions.OpenContext;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * e11-C1 · Async I/O 第一课:orderedWait vs unorderedWait。
 *
 * <p>军规 4 的正解:算子内外呼一律异步化。两种等待模式:
 * unordered(完成即发,吞吐高,顺序打乱)/ ordered(按到达序发,需缓存排队)。
 * 事件时间语义下 unordered 也只在 watermark 间隙内乱序 —— 窗口结果不受影响,
 * 因此**默认选 unordered**,仅当下游依赖严格记录序才用 ordered。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e11-async-io \
 *          -Dexec.mainClass=com.flywhl.flinklab.e11.C1OrderedVsUnorderedJob
 * 预期:UNORD 前缀输出的 seq 序号乱序,ORD 前缀严格递增;两者吞吐差异随延迟方差放大。
 */
public final class C1OrderedVsUnorderedJob {
    private C1OrderedVsUnorderedJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = Labs.events(env, "clicks", 40, 5, 10, 500);

        AsyncDataStream
                .unorderedWait(events, new Enrich("UNORD"), 3, TimeUnit.SECONDS, 50)
                .uid("e11-c1-unordered").print();
        AsyncDataStream
                .orderedWait(events, new Enrich("ORD  "), 3, TimeUnit.SECONDS, 50)
                .uid("e11-c1-ordered").print();

        env.execute("e11-c1-ordered-vs-unordered");
    }

    /** 异步富化:每条外呼一次(50ms 级),回调里 complete。 */
    public static final class Enrich extends RichAsyncFunction<Event, String> {
        private final String tag;
        private transient FakeDimClient client;
        private transient ExecutorService pool;
        private transient long seq;

        public Enrich(String tag) {
            this.tag = tag;
        }

        @Override
        public void open(OpenContext ctx) {
            client = new FakeDimClient(50, 0);
            pool = Executors.newFixedThreadPool(8);
            seq = 0;
        }

        @Override
        public void asyncInvoke(Event e, ResultFuture<String> rf) {
            long mySeq = seq++;
            client.lookup(e.userId, pool).whenComplete((profile, err) -> {
                if (err != null) {
                    rf.completeExceptionally(err);
                } else {
                    rf.complete(Collections.singleton(
                            "[%s] seq=%d user=%s %s".formatted(tag, mySeq, e.userId, profile)));
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
