package com.flywhl.flinklab.e11;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * e11-C4 · 短超时预算 fail-fast：总超时 200ms，慢维表直接降级，强调「预算」而非无限重试。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e11-async-io \
 *          -Dexec.mainClass=com.flywhl.flinklab.e11.C4FailFastBudgetJob
 */
public final class C4FailFastBudgetJob {
    private C4FailFastBudgetJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        AsyncDataStream.unorderedWait(
                        Labs.events(env, "budget", 20, 4, 6, 200),
                        new SlowDim(), 200, TimeUnit.MILLISECONDS, 80)
                .uid("e11-c4-failfast")
                .print();
        env.execute("e11-c4-fail-fast-budget");
    }

    public static final class SlowDim extends RichAsyncFunction<Event, String> {
        private transient FakeDimClient client;
        private transient ExecutorService pool;

        @Override
        public void open(OpenContext ctx) {
            client = new FakeDimClient(120, 0.0); // 基线 120ms，常触 200ms 预算
            pool = Executors.newFixedThreadPool(8);
        }

        @Override
        public void asyncInvoke(Event e, ResultFuture<String> rf) {
            client.lookup(e.userId, pool).whenComplete((p, err) -> {
                if (err != null) {
                    rf.completeExceptionally(err);
                } else {
                    rf.complete(Collections.singleton("OK user=%s %s".formatted(e.userId, p)));
                }
            });
        }

        @Override
        public void timeout(Event e, ResultFuture<String> rf) {
            rf.complete(Collections.singleton(
                    "BUDGET-EXCEEDED user=%s use=default".formatted(e.userId)));
        }

        @Override
        public void close() {
            if (pool != null) {
                pool.shutdown();
            }
        }
    }
}
