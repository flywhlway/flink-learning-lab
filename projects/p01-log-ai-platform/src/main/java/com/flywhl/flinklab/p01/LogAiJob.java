package com.flywhl.flinklab.p01;

import com.flywhl.flinklab.p01.ai.OllamaRiskAsyncFunction;
import com.flywhl.flinklab.p01.cost.BudgetGateFunction;
import com.flywhl.flinklab.p01.enrich.FeatureEnricher;
import com.flywhl.flinklab.p01.model.LogEvent;
import com.flywhl.flinklab.p01.model.LogResult;
import com.flywhl.flinklab.p01.rule.RuleTagger;
import com.flywhl.flinklab.p01.sink.ClickHouseLogSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * p01 日志 AI 平台作业：
 * Kafka {@code logs.events} → Parse → Enrich → Rule → BudgetGate
 * →（allow 时）Async Ollama → Guardrail → ClickHouse。
 *
 * <p>默认 {@code --ai.enabled=false}：零 Ollama 调用，{@code ai_source=DISABLED}（D-04）。
 * 开启时 BudgetGate 短路超限流量；允许路径经 {@link AsyncDataStream#unorderedWaitWithRetry}
 * 旁路宿主机 {@code /api/chat}（D-01 / D-12）。
 *
 * <p>接线顺序强制：Parse→Enrich→Rule→BudgetGate→Async→（Guardrail 由 04-04 Task2）→CH。
 */
public final class LogAiJob {

    private LogAiJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);

        Configuration conf = Configuration.fromMap(Map.of(
                "state.backend.type", cfg.stateBackendType,
                "execution.checkpointing.incremental", "true"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(cfg.checkpointTimeoutMs);

        buildPipeline(env, cfg);
        env.execute(cfg.jobName);
    }

    static DataStream<LogResult> buildPipeline(StreamExecutionEnvironment env, JobConfig cfg) {
        KafkaSource<String> eventSource = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setTopics(cfg.eventsTopic)
                .setGroupId(cfg.groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<LogEvent> events = env
                .fromSource(eventSource, WatermarkStrategy.noWatermarks(), "kafka-log-events")
                .uid("p01-source-log-events")
                .flatMap(new ParseLogJson())
                .name("parse-log-json")
                .uid("p01-parse-log-json")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.eventTime)
                                .withIdleness(Duration.ofSeconds(30)));

        DataStream<LogResult> ruled = events
                .keyBy(e -> e.service == null ? "" : e.service)
                .process(new FeatureEnricher())
                .name("feature-enricher")
                .uid("p01-feature-enricher")
                .map(new RuleTagger())
                .name("rule-tagger")
                .uid("p01-rule-tagger");

        // BudgetGate 标识符必须出现在 AsyncDataStream 之前（源码顺序断言 / D-12）
        BudgetGateFunction budgetGate = new BudgetGateFunction(cfg.budgetMaxAiCalls);

        // D-04：仅 ai.enabled=true 时挂接 BudgetGate + Async Ollama；false 透传 DISABLED
        DataStream<LogResult> results;
        if (cfg.aiEnabled) {
            SingleOutputStreamOperator<LogResult> gated = ruled
                    .keyBy(r -> "_budget")
                    .process(budgetGate)
                    .name("budget-gate")
                    .uid("p01-budget-gate");

            DataStream<LogResult> allowed = gated;
            DataStream<LogResult> tripped = gated.getSideOutput(BudgetGateFunction.DEGRADED_TAG);

            var retryStrategy = new AsyncRetryStrategies
                    .FixedDelayRetryStrategyBuilder<LogResult>(cfg.aiRetry, 200L)
                    .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                    .build();

            DataStream<LogResult> aiResults = AsyncDataStream.unorderedWaitWithRetry(
                            allowed,
                            new OllamaRiskAsyncFunction(cfg.aiEndpoint, cfg.aiModel),
                            cfg.aiTimeoutMs,
                            TimeUnit.MILLISECONDS,
                            cfg.aiCapacity,
                            retryStrategy)
                    .name("ollama-risk-async")
                    .uid("p01-ai-ollama-risk");

            results = aiResults.union(tripped);
        } else {
            results = ruled;
        }

        results
                .sinkTo(new ClickHouseLogSink(
                        cfg.clickhouseUrl, cfg.clickhouseUser, cfg.clickhousePassword))
                .name("clickhouse-log-sink")
                .uid("p01-clickhouse-log-sink");

        return results;
    }
}
