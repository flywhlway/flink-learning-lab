package com.flywhl.flinklab.p02.score;

import com.flywhl.flinklab.p02.catalog.CatalogLoader;
import com.flywhl.flinklab.p02.model.CatalogItem;
import com.flywhl.flinklab.p02.model.FeatureSnapshot;
import com.flywhl.flinklab.p02.model.ItemCatalog;
import com.flywhl.flinklab.p02.model.RecoResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.DefaultJedisClientConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 读 Redis 优先打分；失败/空 → 随流 {@link FeatureSnapshot}，{@code featureSource=STATE_ONLY}（D-01/D-12）。
 *
 * <p>禁止默认抛异常重启作业；正常路径 {@code featureSource=REDIS}。
 */
public final class TopKScoreFunction extends RichFlatMapFunction<FeatureSnapshot, RecoResult> {

    private static final Logger LOG = LoggerFactory.getLogger(TopKScoreFunction.class);

    public static final String SOURCE_REDIS = "REDIS";
    public static final String SOURCE_STATE_ONLY = "STATE_ONLY";

    private static final Pattern SAFE_USER = Pattern.compile("^[A-Za-z0-9_-]+$");

    private final String jdbcUrl;
    private final String redisHost;
    private final int redisPort;
    private final int topK;
    private final int redisTimeoutMs;

    private transient List<CatalogItem> catalogItems;
    private transient Jedis jedis;
    private transient Counter redisReadFailures;
    private transient Counter stateOnlyScores;
    private transient Counter redisScores;

    public TopKScoreFunction(String jdbcUrl, String redisHost, int redisPort, int topK) {
        this(jdbcUrl, redisHost, redisPort, topK, 200);
    }

    public TopKScoreFunction(
            String jdbcUrl, String redisHost, int redisPort, int topK, int redisTimeoutMs) {
        this.jdbcUrl = jdbcUrl;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.topK = Math.max(1, topK);
        this.redisTimeoutMs = Math.max(50, redisTimeoutMs);
    }

    @Override
    public void open(OpenContext ctx) {
        redisReadFailures = getRuntimeContext().getMetricGroup().counter("p02_redis_read_failures");
        stateOnlyScores = getRuntimeContext().getMetricGroup().counter("p02_score_state_only");
        redisScores = getRuntimeContext().getMetricGroup().counter("p02_score_redis");

        Map<String, ItemCatalog> loaded = CatalogLoader.load(jdbcUrl);
        catalogItems = CatalogLoader.toCatalogItems(loaded);
        if (catalogItems.isEmpty()) {
            LOG.warn("catalog 为空，Top-K 将无输出（检查 PG reco_items / pg-jdbc）");
        }

        try {
            JedisClientConfig conf = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(redisTimeoutMs)
                    .socketTimeoutMillis(redisTimeoutMs)
                    .build();
            jedis = new Jedis(redisHost, redisPort, conf);
            jedis.ping();
        } catch (Exception e) {
            LOG.warn("Redis 读连接失败，将使用 STATE_ONLY: {}", e.toString());
            jedis = null;
        }
    }

    @Override
    public void flatMap(FeatureSnapshot value, Collector<RecoResult> out) {
        if (value == null || catalogItems == null || catalogItems.isEmpty()) {
            return;
        }
        FeatureSnapshot features = value;
        String source = SOURCE_STATE_ONLY;

        FeatureSnapshot fromRedis = tryReadRedis(value.userId());
        if (fromRedis != null) {
            features = mergePreferRedis(value, fromRedis);
            source = SOURCE_REDIS;
            redisScores.inc();
        } else {
            stateOnlyScores.inc();
        }

        for (RecoResult r : RuleScorer.scoreToResults(features, catalogItems, topK, source)) {
            out.collect(r);
        }
    }

    /**
     * 点查 {@code feature:{userId}:*}；失败返回 null（不抛）。
     */
    FeatureSnapshot tryReadRedis(String userId) {
        if (userId == null || !SAFE_USER.matcher(userId).matches()) {
            return null;
        }
        try {
            if (jedis == null) {
                JedisClientConfig conf = DefaultJedisClientConfig.builder()
                        .connectionTimeoutMillis(redisTimeoutMs)
                        .socketTimeoutMillis(redisTimeoutMs)
                        .build();
                jedis = new Jedis(redisHost, redisPort, conf);
            }
            String clickKey = "feature:" + userId + ":click_30s";
            String catKey = "feature:" + userId + ":cat_affinity";
            String tsKey = "feature:" + userId + ":last_ts";
            String click = jedis.get(clickKey);
            String cat = jedis.get(catKey);
            String ts = jedis.get(tsKey);
            if (click == null && cat == null && ts == null) {
                return null;
            }
            long clickCount = 0L;
            if (click != null && !click.isBlank()) {
                clickCount = Long.parseLong(click.trim());
            }
            long lastTs = 0L;
            if (ts != null && !ts.isBlank()) {
                lastTs = Long.parseLong(ts.trim());
            }
            Map<String, Double> cats = decodeAffinity(cat);
            // Redis 未存 item 亲和：用空 map，由 merge 回填 State
            return new FeatureSnapshot(userId, cats, Map.of(), lastTs, clickCount);
        } catch (Exception e) {
            redisReadFailures.inc();
            LOG.warn("Redis 点查失败 → STATE_ONLY user={}: {}", userId, e.toString());
            try {
                if (jedis != null) {
                    jedis.close();
                }
            } catch (Exception ignored) {
                // ignore
            }
            jedis = null;
            return null;
        }
    }

    /** Redis 类目/click/ts 优先，item 亲和保留 State 通道。 */
    static FeatureSnapshot mergePreferRedis(FeatureSnapshot state, FeatureSnapshot redis) {
        Map<String, Double> cats = redis.categoryAffinity() != null && !redis.categoryAffinity().isEmpty()
                ? redis.categoryAffinity()
                : state.categoryAffinity();
        long clicks = redis.clickCount() > 0 ? redis.clickCount() : state.clickCount();
        long ts = redis.lastEventTs() > 0 ? redis.lastEventTs() : state.lastEventTs();
        return new FeatureSnapshot(
                state.userId(),
                cats,
                state.itemAffinity(),
                ts,
                clicks);
    }

    static Map<String, Double> decodeAffinity(String encoded) {
        Map<String, Double> map = new HashMap<>();
        if (encoded == null || encoded.isBlank()) {
            return map;
        }
        for (String part : encoded.split(",")) {
            int idx = part.lastIndexOf(':');
            if (idx <= 0 || idx >= part.length() - 1) {
                continue;
            }
            String k = part.substring(0, idx);
            try {
                map.put(k, Double.parseDouble(part.substring(idx + 1)));
            } catch (NumberFormatException ignored) {
                // skip bad token
            }
        }
        return map;
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
            jedis = null;
        }
    }
}
