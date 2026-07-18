package com.flywhl.flinklab.e01;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * e01-J2 · Kafka 端到端点击流窗口统计。
 *
 * <p>链路:{@code clicks}(JSON)→ 解析 → 事件时间 1 分钟 Tumbling Window
 * (按 page 维度统计 PV / UV)→ JSON → {@code clicks.agg}。
 *
 * <p>提交到 Docker 集群(见 docker/Makefile 的 submit-e01):
 * <pre>
 * cd examples && mvn -q clean package
 * cd ../docker && make submit-e01
 * uv run ../scripts/gen_events.py --topic clicks --eps 200
 * </pre>
 *
 * <p>验证:Kafka UI(http://localhost:8080)观察 clicks.agg;
 * Flink UI 观察 watermark 推进与 checkpoint(30s 一次,落 MinIO s3://flink/checkpoints)。
 *
 * <p>语义说明:Sink 采用 AT_LEAST_ONCE。要升级为 EXACTLY_ONCE(Kafka 事务),
 * 必须同时:① sink 侧 setDeliveryGuarantee(EXACTLY_ONCE) 并设置 transactionalIdPrefix;
 * ② broker 的 transaction.max.timeout.ms ≥ sink 的 transaction.timeout.ms > checkpoint 间隔;
 * ③ 下游消费者 isolation.level=read_committed。完整推导见 docs/07-connectors。
 */
public final class KafkaClickstreamWindowJob {

    private KafkaClickstreamWindowJob() {
    }

    public static void main(String[] args) throws Exception {
        // 容器内默认值;本地 -Plocal 运行时可传参:--bootstrap localhost:9094
        String bootstrap = argOr(args, "--bootstrap", "kafka:9092");
        String inTopic   = argOr(args, "--in",  "clicks");
        String outTopic  = argOr(args, "--out", "clicks.agg");
        String groupId   = argOr(args, "--group-id", "e01-clickstream");
        String stateBackend = argOr(args, "--state-backend", "hashmap");
        long checkpointMs = Long.parseLong(argOr(args, "--checkpoint-interval-ms", "30000"));
        boolean unaligned = hasFlag(args, "--unaligned");

        Map<String, String> confMap = new HashMap<>();
        confMap.put("state.backend.type", normalizeBackend(stateBackend));
        if ("rocksdb".equals(normalizeBackend(stateBackend))) {
            confMap.put("execution.checkpointing.incremental", "true");
        }
        if (unaligned) {
            confMap.put("execution.checkpointing.unaligned.enabled", "true");
        }

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(Configuration.fromMap(confMap));
        // 默认 30s EXACTLY_ONCE checkpoint(注意:这是「状态一致性」语义,
        // 端到端语义还取决于 Sink 的 DeliveryGuarantee,二者是两回事)
        env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
        if (unaligned) {
            env.getCheckpointConfig().enableUnalignedCheckpoints();
        }

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<ClickEvent> clicks = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-clicks")
                .uid("e01-j2-source")
                .map(new ParseJson())
                .name("parse-json")
                .uid("e01-j2-parse")
                // 时间戳在解析后基于业务字段分配;withIdleness 防止低流量分区拖死全局 watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.ts)
                                .withIdleness(Duration.ofSeconds(30)));

        clicks.keyBy(e -> e.page)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new PvUvAgg(), new ToJsonResult())
                .name("pv-uv-1m")
                .uid("e01-j2-window")
                .sinkTo(KafkaSink.<String>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(outTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
                .name("kafka-clicks-agg")
                .uid("e01-j2-sink");

        env.execute("e01-kafka-clickstream-window");
    }

    /** JSON → POJO。ObjectMapper 线程安全但初始化昂贵,放在 open() 中每 subtask 一份。 */
    static final class ParseJson extends RichMapFunction<String, ClickEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(OpenContext ctx) {
            mapper = new ObjectMapper();
        }

        @Override
        public ClickEvent map(String json) throws Exception {
            // 教学取舍:解析失败直接抛出以暴露问题。生产写法(脏数据旁路到 side output
            // + 死信 topic)是 e02 的固定案例,勿在此处静默吞异常。
            return mapper.readValue(json, ClickEvent.class);
        }
    }

    /** 累加器:PV 计数 + UV 去重集合。 */
    public static final class PvUv {
        public long pv;
        public Set<String> users = new HashSet<>();
    }

    /**
     * PV/UV 增量聚合。
     *
     * <p>教学取舍:HashSet 精确去重简单直观,但 ① 累加器会走 Kryo 序列化,
     * ② 高基数下窗口状态线性膨胀。生产中 UV 应使用 HyperLogLog 草图或
     * 交给下游 ClickHouse 的 uniqCombined —— 两种改法分别在 e03(状态篇)
     * 与案例一(日志平台)中给出完整实现。
     */
    static final class PvUvAgg implements AggregateFunction<ClickEvent, PvUv, PvUv> {
        @Override
        public PvUv createAccumulator() {
            return new PvUv();
        }

        @Override
        public PvUv add(ClickEvent e, PvUv acc) {
            acc.pv++;
            acc.users.add(e.userId);
            return acc;
        }

        @Override
        public PvUv getResult(PvUv acc) {
            return acc;
        }

        @Override
        public PvUv merge(PvUv a, PvUv b) {
            a.pv += b.pv;
            a.users.addAll(b.users);
            return a;
        }
    }

    /** 输出 JSON,直接兼容 ClickHouse flinklab.click_window_agg 表结构(见 docker/config/clickhouse)。 */
    static final class ToJsonResult
            extends ProcessWindowFunction<PvUv, String, String, TimeWindow> {
        @Override
        public void process(String page, Context ctx, Iterable<PvUv> in, Collector<String> out) {
            PvUv r = in.iterator().next();
            out.collect("""
                    {"window_start":%d,"window_end":%d,"page":"%s","clicks":%d,"users":%d}"""
                    .formatted(ctx.window().getStart(), ctx.window().getEnd(),
                            page, r.pv, r.users.size()));
        }
    }

    private static String normalizeBackend(String raw) {
        String v = raw == null ? "hashmap" : raw.trim().toLowerCase();
        if ("hashmap".equals(v) || "hash_map".equals(v)) {
            return "hashmap";
        }
        if ("rocksdb".equals(v) || "rocks".equals(v)) {
            return "rocksdb";
        }
        return v;
    }

    private static boolean hasFlag(String[] args, String flag) {
        for (String a : args) {
            if (flag.equals(a)) {
                return true;
            }
        }
        return false;
    }

    private static String argOr(String[] args, String key, String dflt) {
        for (int i = 0; i < args.length - 1; i++) {
            if (key.equals(args[i])) {
                return args[i + 1];
            }
        }
        return dflt;
    }
}
