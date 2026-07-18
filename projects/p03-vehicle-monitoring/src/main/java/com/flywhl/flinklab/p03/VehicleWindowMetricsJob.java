package com.flywhl.flinklab.p03;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import com.flywhl.flinklab.p03.sink.ClickHouseWindowMetricsSink;
import com.flywhl.flinklab.p03.window.AttachWindowMeta;
import com.flywhl.flinklab.p03.window.EventCountAgg;
import com.flywhl.flinklab.p03.window.WindowMetricsRow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.Map;

/**
 * p03 旁路窗口指标作业（D-02）：消费同一 {@code vehicle.events}，按 vin 滚动窗口聚合后写入 CH。
 *
 * <p>与 {@link VehicleAlertJob} 解耦：独立 groupId / jobName / 算子 uid 前缀 {@code p03-wm-}；
 * 不改动 CEP / Gate 主图。默认窗口 30s（短压测友好，见 README）。
 */
public final class VehicleWindowMetricsJob {

    /** 演示默认窗口长度；短于 1m 以便 OrbStack 快速落库，README 已声明。 */
    static final Duration WINDOW_SIZE = Duration.ofSeconds(30);

    private VehicleWindowMetricsJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(
                args, "p03-vehicle-window-metrics", "p03-window-metrics");

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
                .uid("p03-wm-source-vehicle-events")
                .flatMap(new ParseVehicleJson())
                .name("parse-vehicle-json")
                .uid("p03-wm-parse-vehicle-json")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.eventTime)
                                .withIdleness(Duration.ofSeconds(30)));

        DataStream<WindowMetricsRow> metrics = events
                .keyBy(e -> e.vin)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .aggregate(new EventCountAgg(), new AttachWindowMeta())
                .name("vin-window-metrics")
                .uid("p03-wm-window-agg");

        metrics
                .sinkTo(new ClickHouseWindowMetricsSink(
                        cfg.clickhouseUrl, cfg.clickhouseUser, cfg.clickhousePassword))
                .name("clickhouse-window-metrics")
                .uid("p03-wm-sink-clickhouse");
    }
}
