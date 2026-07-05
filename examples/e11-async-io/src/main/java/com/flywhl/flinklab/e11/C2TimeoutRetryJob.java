package com.flywhl.flinklab.e11;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.api.common.functions.OpenContext;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * e11-C2 · 超时、重试与降级:外呼可靠性三件套(军规 12 的机制底座)。
 *
 * <p>① 框架级重试:unorderedWaitWithRetry + 固定间隔策略(异常即重试,最多 3 次);
 * ② 总超时:3s 内含全部重试;③ 兜底:timeout() 回调**不再抛异常**而是发降级记录
 * (标记 degraded)——外部系统抖动不再等于作业重启风暴。
 * 该三件套将原样平移到 ai/03 的 LLM 调用(ML_PREDICT 的 DataStream 等价物)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e11-async-io \
 *          -Dexec.mainClass=com.flywhl.flinklab.e11.C2TimeoutRetryJob
 * 预期:约 30% 首查失败,多数经重试成功;少量 DEGRADED 行;作业零重启。
 */
public final class C2TimeoutRetryJob {
    private C2TimeoutRetryJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var retryStrategy = new AsyncRetryStrategies
                .FixedDelayRetryStrategyBuilder<String>(3, 200L)     // 最多 3 次,间隔 200ms
                .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                .build();

        AsyncDataStream.unorderedWaitWithRetry(
                        Labs.events(env, "clicks", 30, 5, 10, 500),
                        new FlakyEnrich(), 3, TimeUnit.SECONDS, 100, retryStrategy)
                .uid("e11-c2-retry")
                .print();

        env.execute("e11-c2-timeout-retry");
    }

    /** 30% 失败率的富化 + 超时降级。 */
    public static final class FlakyEnrich extends RichAsyncFunction<Event, String> {
        private transient FakeDimClient client;
        private transient ExecutorService pool;

        @Override
        public void open(OpenContext ctx) {
            client = new FakeDimClient(40, 0.3);
            pool = Executors.newFixedThreadPool(8);
        }

        @Override
        public void asyncInvoke(Event e, ResultFuture<String> rf) {
            client.lookup(e.userId, pool).whenComplete((p, err) -> {
                if (err != null) {
                    rf.completeExceptionally(err);   // 交给重试策略
                } else {
                    rf.complete(Collections.singleton("OK        user=%s %s".formatted(e.userId, p)));
                }
            });
        }

        @Override
        public void timeout(Event e, ResultFuture<String> rf) {
            // 重试仍未成功:降级而非失败 —— 链路可用性与模型/维表可用性解耦
            rf.complete(Collections.singleton(
                    "DEGRADED  user=%s profile=default(走兜底画像)".formatted(e.userId)));
        }

        @Override
        public void close() {
            if (pool != null) {
                pool.shutdown();
            }
        }
    }
}
