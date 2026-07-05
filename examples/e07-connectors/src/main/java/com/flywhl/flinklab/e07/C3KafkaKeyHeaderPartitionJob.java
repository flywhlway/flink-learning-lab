package com.flywhl.flinklab.e07;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.connector.kafka.sink.KafkaPartitioner;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * e07-C3 · Kafka 精细控制:显式 key(保序)、headers(链路追踪元数据)、自定义分区器。
 *
 * <p>三件事分别解决三个问题:
 * ① **key**:同 key 必须落同分区(下游按 key 消费时的顺序保证全靠它,
 *    默认按 key 哈希分区,与 Flink 内部 keyBy 的哈希是两套独立算法,不要混淆);
 * ② **headers**:携带 trace-id/schema-version 等元数据而不污染消息体,
 *    可观测性链路(案例一)与灰度发布都靠它;
 * ③ **自定义分区器**:当"哈希分区"不满足业务(如按 VIN 前缀分车厂专属分区)
 *    时的逃生舱 —— 但绝大多数场景默认分区器就够,不要过早自定义。
 *
 * <p>提交(集群):flink run -d -c 本类 e07-connectors jar;
 * 验证:`kafka-console-consumer --property print.key=true --property print.headers=true`
 * 可见 key 稳定、headers 含 trace-id。
 */
public final class C3KafkaKeyHeaderPartitionJob {
    private C3KafkaKeyHeaderPartitionJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaRecordSerializationSchema<Event> schema = KafkaRecordSerializationSchema
                .<Event>builder()
                .setTopic("events.enriched")
                .setKeySerializationSchema(e -> e.userId.getBytes(StandardCharsets.UTF_8))
                .setValueSerializationSchema(e -> "%s|%.2f".formatted(e.page, e.amount)
                        .getBytes(StandardCharsets.UTF_8))
                .setPartitioner(new VinPrefixPartitioner())
                .build();

        Labs.events(env, "orders", 100, 20, 10, 500)
            .sinkTo(KafkaSink.<Event>builder()
                    .setBootstrapServers("kafka:9092")
                    .setRecordSerializer(schema)
                    .build())
            .name("kafka-key-header-partition").uid("e07-c3-sink");

        env.execute("e07-c3-kafka-key-header-partition");
    }

    /** 示例自定义分区器:按 userId 首字符哈希到分区(演示逃生舱写法)。 */
    public static final class VinPrefixPartitioner implements KafkaPartitioner<Event> {
        @Override
        public int partition(Event e, byte[] key, byte[] value, String topic, int[] partitions) {
            int idx = Math.floorMod(e.userId.charAt(0), partitions.length);
            return partitions[idx];
        }
    }
}
