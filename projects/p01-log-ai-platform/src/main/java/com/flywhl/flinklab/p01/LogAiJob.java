package com.flywhl.flinklab.p01;

import com.flywhl.flinklab.p01.ai.OllamaRiskAsyncFunction;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * p01 日志 AI 平台作业：
 * Kafka {@code logs.events} → Parse → FeatureEnricher → RuleTagger
 * →（可选）Async Ollama 风险分级 → ClickHouse。
 *
 * <p>默认 {@code --ai.enabled=false}：零 Ollama 调用，{@code ai_source=DISABLED}（D-04）。
 * 开启时经 {@link AsyncDataStream#unorderedWaitWithRetry} 旁路宿主机 {@code /api/chat}（D-01）。
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

        // D-04：仅 ai.enabled=true 时挂接 Async Ollama；false 透传 ai_source=DISABLED
        DataStream<LogResult> results;
        if (cfg.aiEnabled) {
            var retryStrategy = new AsyncRetryStrategies
                    .FixedDelayRetryStrategyBuilder<LogResult>(cfg.aiRetry, 200L)
                    .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                    .build();

            results = AsyncDataStream.unorderedWaitWithRetry(
                            ruled,
                            new OllamaRiskAsyncFunction(cfg.aiEndpoint, cfg.aiModel),
                            cfg.aiTimeoutMs,
                            TimeUnit.MILLISECONDS,
                            cfg.aiCapacity,
                            retryStrategy)
                    .name("ollama-risk-async")
                    .uid("p01-ai-ollama-risk");
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
