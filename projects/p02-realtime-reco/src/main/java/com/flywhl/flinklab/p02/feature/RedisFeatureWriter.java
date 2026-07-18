package com.flywhl.flinklab.p02.feature;

import com.flywhl.flinklab.p02.model.FeatureSnapshot;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Pattern;

/**
 * 双通道特征 · 通道二：jedis Pipeline + {@link CheckpointedFunction} 攒批写 Redis（D-02/D-03）。
 *
 * <p>语义为 <b>at-least-once</b>（非 exactly-once）：checkpoint 尾巴进 Operator ListState，
 * 故障恢复可能重复 SET；同 key 幂等无害。写失败 catch + metric，<b>不抛死作业</b>，
 * 主流 {@link FeatureSnapshot} 继续下发供 STATE_ONLY 打分（T-05-03）。
 *
 * <p>Key 约定（T-05-02）：仅 {@code feature:{validatedUserId}:{suffix}}，禁止原始 JSON 当 key。
 */
public final class RedisFeatureWriter extends RichMapFunction<FeatureSnapshot, FeatureSnapshot>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RedisFeatureWriter.class);

    private static final Pattern SAFE_USER = Pattern.compile("^[A-Za-z0-9_-]+$");

    public static final String KEY_CLICK = "click_30s";
    public static final String KEY_CAT = "cat_affinity";
    public static final String KEY_LAST_TS = "last_ts";

    private final String redisHost;
    private final int redisPort;
    private final int batchThreshold;

    private transient List<FeatureSnapshot> buffer;
    private transient ListState<FeatureSnapshot> checkpointed;
    private transient Jedis jedis;
    private transient Counter writeFailures;
    private transient Counter writeBatches;

    public RedisFeatureWriter(String redisHost, int redisPort, int batchThreshold) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.batchThreshold = Math.max(1, batchThreshold);
    }

    public RedisFeatureWriter(String redisHost, int redisPort) {
        this(redisHost, redisPort, 10);
    }

    @Override
    public void open(OpenContext ctx) {
        buffer = new ArrayList<>();
        writeFailures = getRuntimeContext().getMetricGroup().counter("p02_redis_write_failures");
        writeBatches = getRuntimeContext().getMetricGroup().counter("p02_redis_write_batches");
        try {
            jedis = new Jedis(redisHost, redisPort, 2000);
            jedis.ping();
        } catch (Exception e) {
            LOG.warn("Redis 连接失败，后续写将降级（at-least-once 缓冲仍保留）: {}", e.toString());
            jedis = null;
        }
    }

    @Override
    public FeatureSnapshot map(FeatureSnapshot value) {
        if (value == null || value.userId == null || value.userId.isBlank()) {
            return value;
        }
        if (!SAFE_USER.matcher(value.userId).matches()) {
            LOG.warn("跳过非法 userId 的 Redis 缓冲: {}", value.userId);
            return value;
        }
        buffer.add(value);
        if (buffer.size() >= batchThreshold) {
            flushToRedis();
        }
        // 主流始终透传快照（写失败不影响打分）
        return value;
    }

    /**
     * 构建 Redis key：{@code feature:{userId}:{suffix}}（T-05-02）。
     */
    static String featureKey(String userId, String suffix) {
        if (userId == null || !SAFE_USER.matcher(userId).matches()) {
            throw new IllegalArgumentException("userId 未通过校验，拒绝拼接 Redis key");
        }
        return "feature:" + userId + ":" + suffix;
    }

    private void flushToRedis() {
        if (buffer.isEmpty()) {
            return;
        }
        int n = buffer.size();
        try {
            if (jedis == null) {
                jedis = new Jedis(redisHost, redisPort, 2000);
            }
            Pipeline p = jedis.pipelined();
            for (FeatureSnapshot snap : buffer) {
                String uid = snap.userId;
                p.set(featureKey(uid, KEY_CLICK), Long.toString(snap.clickCount()));
                p.set(featureKey(uid, KEY_CAT), encodeAffinity(snap.categoryAffinity()));
                p.set(featureKey(uid, KEY_LAST_TS), Long.toString(snap.lastEventTs()));
            }
            p.sync();
            writeBatches.inc();
            buffer.clear();
        } catch (Exception e) {
            // at-least-once 降级：不抛；清空缓冲避免无限堆积（状态通道仍可打分）
            writeFailures.inc();
            LOG.warn("Redis Pipeline flush 失败（at-least-once，作业不 FAIL）batch={}: {}", n, e.toString());
            buffer.clear();
            try {
                if (jedis != null) {
                    jedis.close();
                }
            } catch (Exception ignored) {
                // ignore
            }
            jedis = null;
        }
    }

    /** 无引号紧凑编码：cat1:1.0,cat2:3.0（避开 CH/注入字符）。 */
    static String encodeAffinity(Map<String, Double> affinity) {
        if (affinity == null || affinity.isEmpty()) {
            return "";
        }
        StringJoiner joiner = new StringJoiner(",");
        for (Map.Entry<String, Double> e : affinity.entrySet()) {
            String k = e.getKey();
            if (k == null || !SAFE_USER.matcher(k).matches()) {
                continue;
            }
            joiner.add(k + ":" + e.getValue());
        }
        return joiner.toString();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // checkpoint 前刷尾巴；失败仍把 buffer 写入 Operator State（at-least-once）
        if (!buffer.isEmpty()) {
            flushToRedis();
        }
        checkpointed.update(buffer);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointed = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("p02-redis-pending", FeatureSnapshot.class));
        if (buffer == null) {
            buffer = new ArrayList<>();
        }
        if (context.isRestored()) {
            for (FeatureSnapshot snap : checkpointed.get()) {
                buffer.add(snap);
            }
        }
    }

    @Override
    public void close() {
        if (buffer != null && !buffer.isEmpty()) {
            flushToRedis();
        }
        if (jedis != null) {
            jedis.close();
            jedis = null;
        }
    }
}
