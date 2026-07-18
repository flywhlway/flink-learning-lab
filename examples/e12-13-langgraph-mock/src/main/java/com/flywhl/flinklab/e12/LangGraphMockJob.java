package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * ai/第13章 Demo · 外呼 LangGraph/HTTP 的 Mock：Async I/O + 超时降级。
 *
 * <p>不真实发 HTTP。用随机延迟模拟外呼；超过阈值走本地规则降级，
 * 证明「流内编排 vs 外呼工作流」的边界可用 e11 同构机制落地。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-13-langgraph-mock \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.LangGraphMockJob
 */
public final class LangGraphMockJob {
    private LangGraphMockJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        AsyncDataStream.unorderedWait(
                        Labs.events(env, "lg-calls", 10, 4, 6, 200),
                        new MockLangGraphCall(), 1, TimeUnit.SECONDS, 50)
                .uid("e12-13-langgraph-mock")
                .print();

        env.execute("e12-13-langgraph-mock");
    }

    public static final class MockLangGraphCall extends RichAsyncFunction<Event, String> {
        private transient ExecutorService pool;

        @Override
        public void open(OpenContext ctx) {
            pool = Executors.newFixedThreadPool(8);
        }

        @Override
        public void asyncInvoke(Event e, ResultFuture<String> rf) {
            CompletableFuture.supplyAsync(() -> {
                int delay = ThreadLocalRandom.current().nextInt(50, 1500);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (delay > 900) {
                    throw new RuntimeException("mock-http-slow");
                }
                return "REMOTE  user=%s graph=approve delay=%dms".formatted(e.userId, delay);
            }, pool).whenComplete((ok, err) -> {
                if (err != null) {
                    rf.completeExceptionally(err);
                } else {
                    rf.complete(Collections.singleton(ok));
                }
            });
        }

        @Override
        public void timeout(Event e, ResultFuture<String> rf) {
            rf.complete(Collections.singleton(
                    "LOCAL   user=%s graph=rule_fallback (外呼超时降级)".formatted(e.userId)));
        }

        @Override
        public void close() {
            if (pool != null) {
                pool.shutdown();
            }
        }
    }
}
