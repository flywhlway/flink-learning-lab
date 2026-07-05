package com.flywhl.flinklab.e11;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 模拟异步维表客户端:latency 可控、可注入失败(供重试实验)。
 * 生产替身:vertx/lettuce/httpclient 等真正的异步客户端 ——
 * 千万别把同步客户端塞进线程池假装异步(容量被线程数钉死,见 README)。
 */
public final class FakeDimClient implements Serializable {

    private final long baseLatencyMs;
    private final double failRatio;

    public FakeDimClient(long baseLatencyMs, double failRatio) {
        this.baseLatencyMs = baseLatencyMs;
        this.failRatio = failRatio;
    }

    /** 异步查询用户画像;失败以异常完成 future。 */
    public CompletableFuture<String> lookup(String userId, Executor executor) {
        CompletableFuture<String> f = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                Thread.sleep(baseLatencyMs + ThreadLocalRandom.current().nextLong(20));
                if (ThreadLocalRandom.current().nextDouble() < failRatio) {
                    f.completeExceptionally(new RuntimeException("dim timeout(注入)"));
                } else {
                    f.complete("profile-" + userId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                f.completeExceptionally(e);
            }
        });
        return f;
    }
}
