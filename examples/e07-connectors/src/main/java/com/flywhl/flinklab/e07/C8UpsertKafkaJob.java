package com.flywhl.flinklab.e07;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * e07-C8 · upsert-kafka:把回撤流安全落进 Kafka(而不用普通 kafka connector)。
 *
 * <p>问题回顾(e05-C1/C3):无窗口聚合是回撤流(-U/+U),普通 kafka sink 只支持
 * insert-only,直接接会报错。upsert-kafka 的解法:声明 PRIMARY KEY,
 * -U 被丢弃(省一半流量),+U/+I 编码成"新值覆盖旧值"的消息,-D 编码成
 * **value=null 的墓碑消息**(tombstone,Kafka compact topic 的标准删除语义)。
 * 下游按 key compact 语义消费即得到"当前最新状态表"——这是 e08 CDC 落 Kafka
 * 中间层的标准形态(整库同步的事实来源)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e07-connectors \
 *          -Dexec.mainClass=com.flywhl.flinklab.e07.C8UpsertKafkaJob
 * 验证(集群):`kafka-console-consumer --property print.key=true` 观察
 * 同 key 多次覆盖,以及用户下线场景不会在本例出现 tombstone(无 DELETE 语义)。
 */
public final class C8UpsertKafkaJob {
    private C8UpsertKafkaJob() {
    }

    public static void main(String[] args) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        t.executeSql("""
                CREATE TABLE clicks (user_id INT, page STRING)
                WITH ('connector'='datagen','rows-per-second'='10',
                      'fields.user_id.min'='1','fields.user_id.max'='20',
                      'fields.page.length'='1')""");

        t.executeSql("""
                CREATE TABLE user_pv_upsert (
                    user_id INT, pv BIGINT, PRIMARY KEY (user_id) NOT ENFORCED
                ) WITH ('connector'='upsert-kafka',
                        'topic'='user.pv.latest',
                        'properties.bootstrap.servers'='kafka:9092',
                        'key.format'='json','value.format'='json')""");

        t.executeSql("""
                INSERT INTO user_pv_upsert
                SELECT user_id, COUNT(*) FROM clicks GROUP BY user_id""")
         .await();
    }
}
