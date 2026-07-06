package com.flywhl.flinklab.e12;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ai/第18章 Demo · Token 计量流水 + 窗口聚合 + 预算熔断(模拟 token 数,零外部依赖)。
 *
 * <p>模拟每个租户的 LLM 调用产生 token 消耗记录(真实场景中这条记录由
 * AI 网关或 Agent 在每次调用后产出,此处用随机数代替避免依赖真实模型服务)。
 * 按租户 1 分钟滚动窗口聚合成本,超过预算阈值即输出熔断信号。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-18-streaming-cost-control \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingCostControlJob
 * 验证:tenant-A(高频租户)应比 tenant-B 更快触发 BUDGET-EXCEEDED。
 */
public final class StreamingCostControlJob {
    private StreamingCostControlJob() {
    }

    public static class TokenUsageRecord {
        public String tenantId;
        public long promptTokens, completionTokens;
        public double costUsd;

        public TokenUsageRecord() {
        }

        public TokenUsageRecord(String tenantId, long p, long c, double cost) {
            this.tenantId = tenantId; this.promptTokens = p; this.completionTokens = c; this.costUsd = cost;
        }
    }

    private static final double BUDGET_PER_MINUTE_USD = 0.05;   // 演示阈值,刻意设低便于触发

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        GeneratorFunction<Long, TokenUsageRecord> gen = idx -> {
            var rnd = ThreadLocalRandom.current();
            String tenant = rnd.nextDouble() < 0.7 ? "tenant-A" : "tenant-B";   // A 是高频租户
            long promptTok = rnd.nextLong(100, 500);
            long compTok = rnd.nextLong(50, 300);
            double cost = (promptTok + compTok) * 0.00001;   // 模拟单价
            return new TokenUsageRecord(tenant, promptTok, compTok, cost);
        };
        DataStream<TokenUsageRecord> usage = env.fromSource(
                new DataGeneratorSource<>(gen, Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(10), TypeInformation.of(TokenUsageRecord.class)),
                WatermarkStrategy.noWatermarks(), "token-usage-source");

        // 计量流水打印(生产版本落 ClickHouse,复用 e07-C6 骨架)
        usage.map(r -> "USAGE  tenant=%s cost=$%.5f".formatted(r.tenantId, r.costUsd))
             .uid("e12-18-usage-log")
             .print();

        // 窗口聚合 + 预算熔断(e02 滚动窗口模式复用)
        usage.assignTimestampsAndWatermarks(
                    WatermarkStrategy.<TokenUsageRecord>forMonotonousTimestamps()
                        .withTimestampAssigner((r, ts) -> System.currentTimeMillis()))
             .keyBy(r -> r.tenantId)
             .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
             .aggregate(new CostSumAgg(), new BudgetEnforcer())
             .uid("e12-18-budget-check")
             .print();

        env.execute("e12-18-streaming-cost-control");
    }

    /** 窗口内成本累加(简单求和,e02 增量聚合模式)。 */
    public static final class CostSumAgg
            implements AggregateFunction<TokenUsageRecord, Double, Double> {
        @Override public Double createAccumulator() { return 0.0; }
        @Override public Double add(TokenUsageRecord r, Double acc) { return acc + r.costUsd; }
        @Override public Double getResult(Double acc) { return acc; }
        @Override public Double merge(Double a, Double b) { return a + b; }
    }

    /** 预算熔断判断:超阈值输出告警,否则输出常规汇总。 */
    public static final class BudgetEnforcer
            extends ProcessWindowFunction<Double, String, String, TimeWindow> {
        @Override
        public void process(String tenant, Context ctx, Iterable<Double> totals, Collector<String> out) {
            double total = totals.iterator().next();
            if (total > BUDGET_PER_MINUTE_USD) {
                out.collect("BUDGET-EXCEEDED  tenant=%s window_cost=$%.5f > budget=$%.5f → 建议:切换降级模型/限流"
                        .formatted(tenant, total, BUDGET_PER_MINUTE_USD));
            } else {
                out.collect("OK  tenant=%s window_cost=$%.5f (budget=$%.5f)"
                        .formatted(tenant, total, BUDGET_PER_MINUTE_USD));
            }
        }
    }
}
