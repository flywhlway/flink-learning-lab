package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * ai/第06章 Demo · 实时特征双通道:滑动窗口统计特征 + 会话级状态特征 → Redis。
 *
 * <p>通道一复用 e02 滑动窗口模式(近 30 秒点击数);通道二复用 e03-C3
 * MapState 模式(会话内品类计数);写入复用 e07-C7 的 jedis Pipeline 攒批模式。
 * 本 Demo 是三个已验证模式在"特征工程"场景的组合应用,零 Preview API 依赖。
 *
 * <p>前置:docker/ 环境需已启动 redis(`make up`)。
 * 运行:mvn -q -Plocal compile exec:java -pl e12-06-streaming-feature \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingFeatureJob
 * 验证:redis-cli KEYS 'feature:*' 可见两类特征 key 持续更新。
 */
public final class StreamingFeatureJob {
    private StreamingFeatureJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = Labs.events(env, "behavior", 30, 6, 10, 500)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)));

        // 通道一:滑动窗口统计特征(近 30 秒点击数,滑动 10 秒)—— e02 模式复用
        events.keyBy(e -> e.userId)
              .window(SlidingEventTimeWindows.of(Duration.ofSeconds(30), Duration.ofSeconds(10)))
              .aggregate(new ClickCountAgg())
              .map(kv -> "feature:click_30s|" + kv)
              .map(new RedisFeatureWriter(1))   // 攒批阈值小,便于观察
              .uid("e12-06-window-feature")
              .print();

        // 通道二:会话级状态特征(累计品类点击次数)—— e03-C3 模式复用
        events.keyBy(e -> e.userId)
              .process(new SessionCategoryFeature())
              .map(new RedisFeatureWriter(5))
              .uid("e12-06-session-feature")
              .print();

        env.execute("e12-06-streaming-feature");
    }

    /** 窗口内计数(带 key 前缀输出,便于区分特征名)。 */
    public static final class ClickCountAgg
            implements AggregateFunction<Event, Long, String> {
        // 简化:这里不做 key 关联,生产版本应在 ProcessWindowFunction 中带出 userId
        @Override public Long createAccumulator() { return 0L; }
        @Override public Long add(Event e, Long acc) { return acc + 1; }
        @Override public String getResult(Long acc) { return "count=" + acc; }
        @Override public Long merge(Long a, Long b) { return a + b; }
    }

    /** 会话级品类计数特征(MapState,e03-C3 同款模式)。 */
    public static final class SessionCategoryFeature
            extends KeyedProcessFunction<String, Event, String> {
        private transient MapState<String, Integer> categoryCount;

        @Override
        public void open(OpenContext ctx) {
            categoryCount = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("session-category", String.class, Integer.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            int c = (categoryCount.contains(e.page) ? categoryCount.get(e.page) : 0) + 1;
            categoryCount.put(e.page, c);
            out.collect("feature:session_category|user=%s cat=%s cnt=%d"
                    .formatted(e.userId, e.page, c));
        }
    }

    /** Redis 攒批写(jedis Pipeline,e07-C7 同款容错模式的简化教学版:本 Demo 省略 Operator State 部分)。 */
    public static final class RedisFeatureWriter
            implements org.apache.flink.api.common.functions.MapFunction<String, String> {
        private final int threshold;
        private transient List<String> buffer;
        private transient Jedis jedis;

        public RedisFeatureWriter(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public String map(String featureLine) {
            if (buffer == null) {
                buffer = new ArrayList<>();
                jedis = new Jedis("localhost", 6379);   // 本地直连;集群内替换为 "redis"
            }
            buffer.add(featureLine);
            if (buffer.size() >= threshold) {
                Pipeline p = jedis.pipelined();
                for (String line : buffer) {
                    String[] parts = line.split("\\|", 2);
                    p.set(parts[0] + ":" + System.nanoTime(), parts.length > 1 ? parts[1] : "");
                }
                p.sync();
                int n = buffer.size();
                buffer.clear();
                return "FLUSH %d features → redis".formatted(n);
            }
            return "buffering(%d/%d): %s".formatted(buffer.size(), threshold, featureLine);
        }
    }
}
