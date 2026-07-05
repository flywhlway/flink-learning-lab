package com.flywhl.flinklab.e04;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * e04-C2 · Kafka 端到端 EXACTLY_ONCE:两阶段提交的完整配置(集群运行)。
 *
 * <p>三个必须同时满足的条件(缺一即静默降级或事故):
 * ① Sink:EXACTLY_ONCE + transactionalIdPrefix(唯一,跨作业不可撞);
 * ② 超时链:checkpoint间隔(30s) < sink transaction.timeout.ms(10min)
 *    ≤ broker transaction.max.timeout.ms(默认15min)——事务必须活过
 *    "两次 checkpoint 之间 + 恢复耗时",否则 broker 中止事务 = 已预写数据丢失;
 * ③ 下游:isolation.level=read_committed,否则读到未提交数据,一切白费。
 *
 * <p>提交与验证:
 * <pre>
 * cd examples && mvn -q clean package
 * cp e04-checkpoint/target/e04-checkpoint-*.jar ../docker/jobs/
 * cd ../docker
 * docker compose exec jobmanager flink run -d \
 *     -c com.flywhl.flinklab.e04.C2KafkaExactlyOnceJob \
 *     /opt/flink/usrlib/e04-checkpoint-0.1.0.jar
 * # read_committed 消费:输出按 checkpoint 节拍「一批一批」出现,而非连续流出
 * docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
 *     --bootstrap-server localhost:9092 --topic events.enriched \
 *     --isolation-level read_committed
 * # 对照:read_uncommitted(默认)则连续流出 —— 亲眼看见事务边界
 * </pre>
 */
public final class C2KafkaExactlyOnceJob {
    private C2KafkaExactlyOnceJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        Labs.events(env, "orders", 100, 20, 10, 1_000)
            .map(e -> "{\"user\":\"%s\",\"page\":\"%s\",\"amount\":%.2f,\"ts\":%d}"
                    .formatted(e.userId, e.page, e.amount, e.ts))
            .uid("e04-c2-json")
            .sinkTo(KafkaSink.<String>builder()
                    .setBootstrapServers("kafka:9092")
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("events.enriched")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .setTransactionalIdPrefix("e04-c2-2pc")
                    .setProperty("transaction.timeout.ms", "600000")
                    .build())
            .name("kafka-exactly-once").uid("e04-c2-sink");

        env.execute("e04-c2-kafka-exactly-once");
    }
}
