package com.flywhl.flinklab.e07;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * e07-C1 · Kafka 三级投递语义一键切换(--mode none|alo|eos),配合 kill TM 观察差异。
 *
 * <p>语义矩阵(故障恢复后 topic 里的表现):
 * NONE=可能丢+可能重 | AT_LEAST_ONCE=不丢但重(flush 于 checkpoint)|
 * EXACTLY_ONCE=不丢不重(2PC,须 read_committed,可见性延迟=ckpt 间隔)。
 * 实验:提交后 `docker compose kill taskmanager && docker compose up -d taskmanager`,
 * 分别数三种 mode 下输出 topic 的消息量与重复率。
 *
 * <p>提交:构建后 cp 到 docker/jobs,flink run -d -c 本类 jar --mode alo
 */
public final class C1KafkaDeliveryMatrixJob {
    private C1KafkaDeliveryMatrixJob() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length >= 2 && "--mode".equals(args[0]) ? args[1] : "alo";
        DeliveryGuarantee g = switch (mode) {
            case "none" -> DeliveryGuarantee.NONE;
            case "eos"  -> DeliveryGuarantee.EXACTLY_ONCE;
            default     -> DeliveryGuarantee.AT_LEAST_ONCE;
        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(15_000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSink.Builder<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("events.enriched")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(g);
        if (g == DeliveryGuarantee.EXACTLY_ONCE) {
            sink.setTransactionalIdPrefix("e07-c1-" + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", "600000");
        }

        Labs.events(env, "orders", 200, 20, 10, 500)
            .map(e -> "%s|%s|%.2f|%d".formatted(e.userId, e.page, e.amount, e.ts))
            .uid("e07-c1-fmt")
            .sinkTo(sink.build())
            .name("kafka-" + mode).uid("e07-c1-sink");

        env.execute("e07-c1-kafka-delivery-" + mode);
    }
}
