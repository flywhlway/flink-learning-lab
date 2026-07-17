package com.flywhl.flinklab.p03;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flywhl.flinklab.p03.cep.AlertPatternHandler;
import com.flywhl.flinklab.p03.cep.HarshThenFaultPattern;
import com.flywhl.flinklab.p03.model.AlertEvent;
import com.flywhl.flinklab.p03.model.VehicleEvent;
import com.flywhl.flinklab.p03.sink.ClickHouseAlertSink;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.CEP;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * p03 车联网告警作业：Kafka → parse → WM → keyBy(vin) → CEP → 双写 Kafka + ClickHouse。
 *
 * <p>MATCH 主流与 TIMEOUT Side Output 同表 {@code alert_type}（RESOLVED Q3）。
 */
public final class VehicleAlertJob {

    private VehicleAlertJob() {
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

    static void buildPipeline(StreamExecutionEnvironment env, JobConfig cfg) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setTopics(cfg.eventsTopic)
                .setGroupId(cfg.groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<VehicleEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-vehicle-events")
                .uid("p03-source-vehicle-events")
                .flatMap(new ParseVehicleJson())
                .name("parse-vehicle-json")
                .uid("p03-parse-vehicle-json")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.eventTime)
                                .withIdleness(Duration.ofSeconds(30)));

        AlertPatternHandler handler = new AlertPatternHandler();

        SingleOutputStreamOperator<AlertEvent> matched = CEP
                .pattern(events.keyBy(e -> e.vin), HarshThenFaultPattern.build())
                .process(handler)
                .name("cep-harsh-then-fault")
                .uid("p03-cep-harsh-then-fault");

        DataStream<AlertEvent> timeouts = matched.getSideOutput(AlertPatternHandler.TIMEOUT_TAG);
        DataStream<AlertEvent> allAlerts = matched.union(timeouts);

        KafkaSink<AlertEvent> kafkaSink = KafkaSink.<AlertEvent>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(cfg.alertsTopic)
                        .setValueSerializationSchema(new AlertEventJsonSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        allAlerts
                .sinkTo(kafkaSink)
                .name("kafka-vehicle-alerts")
                .uid("p03-sink-kafka-alerts");

        allAlerts
                .sinkTo(new ClickHouseAlertSink(
                        cfg.clickhouseUrl, cfg.clickhouseUser, cfg.clickhousePassword))
                .name("clickhouse-vehicle-alerts")
                .uid("p03-sink-clickhouse-alerts");
    }

    /** AlertEvent → JSON 字节，供 KafkaSink 序列化。 */
    static final class AlertEventJsonSchema implements SerializationSchema<AlertEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(InitializationContext context) {
            mapper = new ObjectMapper();
        }

        @Override
        public byte[] serialize(AlertEvent element) {
            try {
                if (mapper == null) {
                    mapper = new ObjectMapper();
                }
                return mapper.writeValueAsString(element).getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new IllegalStateException("序列化 AlertEvent 失败", e);
            }
        }
    }
}
