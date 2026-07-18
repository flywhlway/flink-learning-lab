package com.flywhl.flinklab.p03;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flywhl.flinklab.p03.cep.AlertPatternHandler;
import com.flywhl.flinklab.p03.cep.DtcPairHandler;
import com.flywhl.flinklab.p03.cep.DtcPairPattern;
import com.flywhl.flinklab.p03.cep.HarshThenFaultHandler;
import com.flywhl.flinklab.p03.cep.HarshThenFaultPattern;
import com.flywhl.flinklab.p03.cep.PatternActivationGate;
import com.flywhl.flinklab.p03.cep.TripleHarshHandler;
import com.flywhl.flinklab.p03.cep.TripleHarshPattern;
import com.flywhl.flinklab.p03.model.AlertEvent;
import com.flywhl.flinklab.p03.model.PatternControlMessage;
import com.flywhl.flinklab.p03.model.VehicleEvent;
import com.flywhl.flinklab.p03.sink.ClickHouseAlertSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * p03 车联网告警作业：三路预编译 CEP + Broadcast 出口门控 + 双写 Kafka/ClickHouse。
 *
 * <p>静态并行挂载 {@code HARSH_THEN_FAULT} / {@code TRIPLE_HARSH} / {@code DTC_PAIR}；
 * 控制面消费 {@code vehicle.pattern.control}，经 {@link PatternActivationGate} 按激活集过滤后再双写（D-04/D-07）。
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
        KafkaSource<String> eventSource = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setTopics(cfg.eventsTopic)
                .setGroupId(cfg.groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<VehicleEvent> events = env
                .fromSource(eventSource, WatermarkStrategy.noWatermarks(), "kafka-vehicle-events")
                .uid("p03-source-vehicle-events")
                .flatMap(new ParseVehicleJson())
                .name("parse-vehicle-json")
                .uid("p03-parse-vehicle-json")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.eventTime)
                                .withIdleness(Duration.ofSeconds(30)));

        SingleOutputStreamOperator<AlertEvent> htf = CEP
                .pattern(events.keyBy(e -> e.vin), HarshThenFaultPattern.build())
                .process(new HarshThenFaultHandler())
                .name("cep-harsh-then-fault")
                .uid("p03-cep-harsh-then-fault");

        SingleOutputStreamOperator<AlertEvent> triple = CEP
                .pattern(events.keyBy(e -> e.vin), TripleHarshPattern.build())
                .process(new TripleHarshHandler())
                .name("cep-triple-harsh")
                .uid("p03-cep-triple-harsh");

        SingleOutputStreamOperator<AlertEvent> dtcPair = CEP
                .pattern(events.keyBy(e -> e.vin), DtcPairPattern.build())
                .process(new DtcPairHandler())
                .name("cep-dtc-pair")
                .uid("p03-cep-dtc-pair");

        DataStream<AlertEvent> allAlerts = htf
                .union(htf.getSideOutput(AlertPatternHandler.TIMEOUT_TAG))
                .union(triple)
                .union(triple.getSideOutput(AlertPatternHandler.TIMEOUT_TAG))
                .union(dtcPair)
                .union(dtcPair.getSideOutput(AlertPatternHandler.TIMEOUT_TAG));

        KafkaSource<String> controlSource = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setTopics(cfg.controlTopic)
                .setGroupId(cfg.groupId + "-control")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        BroadcastStream<PatternControlMessage> controlBroadcast = env
                .fromSource(
                        controlSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-pattern-control")
                .uid("p03-source-pattern-control")
                .flatMap(new ParsePatternControlJson())
                .name("parse-pattern-control")
                .uid("p03-parse-pattern-control")
                .broadcast(PatternActivationGate.ACTIVE_DESC);

        DataStream<AlertEvent> gated = allAlerts
                .connect(controlBroadcast)
                .process(new PatternActivationGate(PatternActivationGate.ACTIVE_DESC))
                .name("gate-pattern-activation")
                .uid("p03-gate-pattern-activation");

        KafkaSink<AlertEvent> kafkaSink = KafkaSink.<AlertEvent>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(cfg.alertsTopic)
                        .setValueSerializationSchema(new AlertEventJsonSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        gated
                .sinkTo(kafkaSink)
                .name("kafka-vehicle-alerts")
                .uid("p03-sink-kafka-alerts");

        gated
                .sinkTo(new ClickHouseAlertSink(
                        cfg.clickhouseUrl, cfg.clickhouseUser, cfg.clickhousePassword))
                .name("clickhouse-vehicle-alerts")
                .uid("p03-sink-clickhouse-alerts");
    }

    /** AlertEvent → JSON 字节，供 KafkaSink 序列化（含 patternId）。 */
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
