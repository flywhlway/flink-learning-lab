package com.flywhl.flinklab.e08;

import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder.PostgresIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * e08-C4 · Postgres CDC 编程接口(DataStream):与 p01 YAML 同源不同壳。
 *
 * <p>YAML Pipeline(p01-p03)面向"整库同步治理"场景,声明式、免编译;
 * 本例面向"CDC 数据需要接入自定义 DataStream 逻辑"场景(如 CEP 检测 CDC 事件序列、
 * 复杂状态计算)——两者共享同一套底层增量快照算法(Chunk 切分 + 无锁读),
 * 只是暴露接口层次不同。核心机制:**增量快照(Incremental Snapshot Framework)**
 * 把"全量导出"切成多个可并行、可 checkpoint 的 chunk,不再需要传统 CDC 方案
 * 那样为保证一致性而对源表加全局锁——这是 Flink CDC 相对 Debezium 原生的核心改进。
 *
 * <p>提交(集群):flink run -d -c 本类 e08-cdc jar;
 * 验证:先跑通 sql/pg-init.sql,启动后应先看到 100 条全量快照记录(op=r 或 c),
 * 随后 `UPDATE orders SET status='PAID' WHERE order_id=1` 应实时出现一条 op=u 记录。
 */
public final class C4PostgresCdcDataStreamJob {
    private C4PostgresCdcDataStreamJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);   // chunk 分配与已完成 chunk 位点均随 checkpoint 持久化

        PostgresIncrementalSource<String> source = PostgresSourceBuilder.PostgresIncrementalSource
                .<String>builder()
                .hostname("postgres")
                .port(5432)
                .database("flinklab")
                .schemaList("public")
                .tableList("public.orders")
                .username("flinklab")
                .password("flinklab123")
                .slotName("flink_cdc_slot_c4")
                .decodingPluginName("pgoutput")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "postgres-cdc-source")
           .uid("e08-c4-cdc-source")
           .print();

        env.execute("e08-c4-postgres-cdc-datastream");
    }
}
