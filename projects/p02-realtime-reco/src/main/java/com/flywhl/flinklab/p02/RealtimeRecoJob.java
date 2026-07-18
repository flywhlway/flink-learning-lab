package com.flywhl.flinklab.p02;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flywhl.flinklab.p02.feature.RedisFeatureWriter;
import com.flywhl.flinklab.p02.feature.SessionFeatureFunction;
import com.flywhl.flinklab.p02.model.BehaviorEvent;
import com.flywhl.flinklab.p02.model.FeatureSnapshot;
import com.flywhl.flinklab.p02.model.RecoResult;
import com.flywhl.flinklab.p02.score.TopKScoreFunction;
import com.flywhl.flinklab.p02.sink.ClickHouseRecoSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * p02 实时推荐作业（RECO-02）：
 * Kafka reco.events → Parse → SessionFeature → RedisFeatureWriter → TopK →
 * 双写 Kafka reco.results + ClickHouse reco_results。
 *
 * <p>Redis 写为 at-least-once（非 exactly-once）；读失败 STATE_ONLY，作业不 FAIL。
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
     * 完整图：Parse → 双通道特征 → Top-K → Kafka/CH 双写。
     *
     * @return 推荐结果流（供测试挂接）
     */
    static DataStream<RecoResult> buildPipeline(StreamExecutionEnvironment env, JobConfig cfg) {
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

        DataStream<FeatureSnapshot> features = events
                .keyBy(e -> e.userId)
                .process(new SessionFeatureFunction(cfg.postgresJdbcUrl))
                .name("session-feature")
                .uid("p02-session-feature");

        DataStream<FeatureSnapshot> afterRedis = features
                .map(new RedisFeatureWriter(cfg.redisHost, cfg.redisPort, 5))
                .name("redis-feature-writer")
                .uid("p02-redis-feature-writer");

        DataStream<RecoResult> results = afterRedis
                .flatMap(new TopKScoreFunction(
                        cfg.postgresJdbcUrl, cfg.redisHost, cfg.redisPort, cfg.topK))
                .name("topk-score")
                .uid("p02-topk-score");

        KafkaSink<RecoResult> kafkaSink = KafkaSink.<RecoResult>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(cfg.resultsTopic)
                        .setValueSerializationSchema(new RecoResultJsonSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        results
                .sinkTo(kafkaSink)
                .name("kafka-reco-results")
                .uid("p02-sink-kafka-results");

        results
                .sinkTo(new ClickHouseRecoSink(
                        cfg.clickhouseUrl, cfg.clickhouseUser, cfg.clickhousePassword))
                .name("clickhouse-reco-results")
                .uid("p02-sink-clickhouse-results");

        return results;
    }

    /** RecoResult → JSON，供 KafkaSink（字段无引号污染由 Sink 侧已校验）。 */
    static final class RecoResultJsonSchema implements SerializationSchema<RecoResult> {
        private transient ObjectMapper mapper;

        @Override
        public void open(InitializationContext context) {
            mapper = new ObjectMapper();
        }

        @Override
        public byte[] serialize(RecoResult element) {
            try {
                if (mapper == null) {
                    mapper = new ObjectMapper();
                }
                return mapper.writeValueAsString(element).getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new IllegalStateException("序列化 RecoResult 失败", e);
            }
        }
    }
}
