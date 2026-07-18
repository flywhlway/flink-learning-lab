package com.flywhl.flinklab.p01;

import com.flywhl.flinklab.p01.model.LogEvent;
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
 * p01 日志 AI 平台作业（V1 骨架）：Kafka {@code logs.events} → {@link ParseLogJson} 透传。
 *
 * <p>本切片默认 {@code --ai.enabled=false}，不接 Agents/Milvus/CH Sink；
 * V2（04-02）将接线 FeatureEnricher / RuleTagger / ClickHouse Sink。
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

    static DataStream<LogEvent> buildPipeline(StreamExecutionEnvironment env, JobConfig cfg) {
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

        // V1：无 CH Sink；print 保持图连通以便 flink run 启动。V2（04-02）将接 ClickHouseLogSink。
        events
                .map(e -> e.service + "|" + e.level + "|" + e.traceId)
                .name("passthrough-await-v2-sink")
                .uid("p01-passthrough-await-v2-sink")
                .print()
                .uid("p01-print-await-v2-ch-sink");

        return events;
    }
}
