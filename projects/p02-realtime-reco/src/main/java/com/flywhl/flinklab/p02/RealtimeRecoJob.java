package com.flywhl.flinklab.p02;

import com.flywhl.flinklab.p02.model.BehaviorEvent;
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
 * p02 实时推荐作业（RECO-01 透传切片）：
 * Kafka {@code reco.events} → {@link ParseBehaviorJson} → print 占位。
 *
 * <p>特征双通道 / Top-K / CH 双写由 05-02 接线；本切片证明行为流可接入（D-07/D-09）。
 */
public final class RealtimeRecoJob {

    private RealtimeRecoJob() {
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

    /**
     * Kafka → Parse 透传；返回 BehaviorEvent 流供后续 05-02 挂接特征/打分。
     */
    static DataStream<BehaviorEvent> buildPipeline(StreamExecutionEnvironment env, JobConfig cfg) {
        KafkaSource<String> eventSource = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setTopics(cfg.eventsTopic)
                .setGroupId(cfg.groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<BehaviorEvent> events = env
                .fromSource(eventSource, WatermarkStrategy.noWatermarks(), "kafka-reco-events")
                .uid("p02-source-reco-events")
                .flatMap(new ParseBehaviorJson())
                .name("parse-behavior-json")
                .uid("p02-parse-behavior")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<BehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.eventTime)
                                .withIdleness(Duration.ofSeconds(30)));

        // 05-01 占位：侧效应无关的透传观测；05-02 替换为特征/打分/双写
        events
                .map(e -> e.userId + "|" + e.itemId + "|" + e.eventType + "|" + e.eventTime)
                .name("passthrough-print")
                .uid("p02-passthrough-print")
                .print();

        return events;
    }
}
