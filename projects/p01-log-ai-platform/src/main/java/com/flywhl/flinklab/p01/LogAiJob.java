package com.flywhl.flinklab.p01;

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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Map;

/**
 * p01 日志 AI 平台作业（V2 规则路径）：
 * Kafka {@code logs.events} → Parse → FeatureEnricher → RuleTagger → ClickHouse。
 *
 * <p>默认 {@code --ai.enabled=false}：零 Ollama/Milvus 调用，{@code ai_source=DISABLED}（D-04）。
 * Async AI 旁路留给 04-03。
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

        // V2：规则路径（AI off 旁路；本切片不接 Async Ollama）
        DataStream<LogResult> results = events
                .keyBy(e -> e.service == null ? "" : e.service)
                .process(new FeatureEnricher())
                .name("feature-enricher")
                .uid("p01-feature-enricher")
                .map(new RuleTagger())
                .name("rule-tagger")
                .uid("p01-rule-tagger");

        results
                .sinkTo(new ClickHouseLogSink(
                        cfg.clickhouseUrl, cfg.clickhouseUser, cfg.clickhousePassword))
                .name("clickhouse-log-sink")
                .uid("p01-clickhouse-log-sink");

        return results;
    }
}
