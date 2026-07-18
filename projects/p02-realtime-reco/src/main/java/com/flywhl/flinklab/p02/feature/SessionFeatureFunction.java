package com.flywhl.flinklab.p02.feature;

import com.flywhl.flinklab.p02.catalog.CatalogLoader;
import com.flywhl.flinklab.p02.model.BehaviorEvent;
import com.flywhl.flinklab.p02.model.FeatureSnapshot;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 双通道特征 · 通道一：Keyed State 累积会话/近窗特征（D-01）。
 *
 * <p>{@code keyBy(userId)} 后维护 item/category 亲和与 clickCount；
 * 每条事件输出携带 {@link FeatureSnapshot} 的快照供 Redis 写与打分降级。
 *
 * <p>类目亲和依赖上游可选 category（作业侧由 catalog 反查 item→category 注入）。
 */
public final class SessionFeatureFunction
        extends KeyedProcessFunction<String, BehaviorEvent, FeatureSnapshot> {

    /** RESEARCH 权重：VIEW=1 CLICK=3 CART=5 BUY=10。 */
    public static double weightOf(String eventType) {
        if (eventType == null) {
            return 0.0;
        }
        return switch (eventType) {
            case "VIEW" -> 1.0;
            case "CLICK" -> 3.0;
            case "CART" -> 5.0;
            case "BUY" -> 10.0;
            default -> 0.0;
        };
    }

    private transient MapState<String, Double> itemAffinityState;
    private transient MapState<String, Double> categoryAffinityState;
    private transient ValueState<Long> lastEventTsState;
    private transient ValueState<Long> clickCountState;

    /** itemId → category；open 时从 PG 加载，或由 {@link #withCategoryIndex} 预置。 */
    private Map<String, String> categoryIndex;

    private final String jdbcUrl;

    public SessionFeatureFunction() {
        this.jdbcUrl = null;
    }

    public SessionFeatureFunction(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /** 注入 item→category 索引（单测 / 预置；优先于 jdbc 加载结果）。 */
    public SessionFeatureFunction withCategoryIndex(Map<String, String> categoryIndex) {
        this.categoryIndex = categoryIndex == null ? null : Map.copyOf(categoryIndex);
        return this;
    }

    @Override
    public void open(OpenContext ctx) {
        itemAffinityState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("p02-item-affinity", String.class, Double.class));
        categoryAffinityState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("p02-cat-affinity", String.class, Double.class));
        lastEventTsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("p02-last-ts", Long.class));
        clickCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("p02-click-count", Long.class));
        if ((categoryIndex == null || categoryIndex.isEmpty()) && jdbcUrl != null) {
            categoryIndex = CatalogLoader.categoryIndex(CatalogLoader.load(jdbcUrl));
        }
    }

    @Override
    public void processElement(BehaviorEvent event, Context ctx, Collector<FeatureSnapshot> out)
            throws Exception {
        String category = null;
        if (categoryIndex != null && event.itemId != null) {
            category = categoryIndex.get(event.itemId);
        }
        FeatureSnapshot prev = readSnapshot(event.userId);
        FeatureSnapshot next = applyEvent(prev, event, category);
        writeSnapshot(next);
        out.collect(next);
    }

    /**
     * 纯逻辑累积入口（单测 / 作业共用）：在 prev 上叠加一条行为。
     *
     * @param category 可为 null（跳过类目亲和更新）
     */
    public static FeatureSnapshot applyEvent(
            FeatureSnapshot prev, BehaviorEvent event, String category) {
        if (event == null) {
            return prev != null ? prev : new FeatureSnapshot();
        }
        Map<String, Double> items = prev != null
                ? new HashMap<>(prev.itemAffinity())
                : new HashMap<>();
        Map<String, Double> cats = prev != null
                ? new HashMap<>(prev.categoryAffinity())
                : new HashMap<>();
        long clicks = prev != null ? prev.clickCount() : 0L;

        double w = weightOf(event.eventType);
        if (w > 0 && event.itemId != null) {
            items.merge(event.itemId, w, Double::sum);
        }
        if (w > 0 && category != null && !category.isBlank()) {
            cats.merge(category, w, Double::sum);
        }
        if ("CLICK".equals(event.eventType)) {
            clicks += 1;
        }

        return new FeatureSnapshot(
                event.userId,
                cats,
                items,
                event.eventTime,
                clicks);
    }

    private FeatureSnapshot readSnapshot(String userId) throws Exception {
        Map<String, Double> items = new HashMap<>();
        Map<String, Double> cats = new HashMap<>();
        if (itemAffinityState != null) {
            for (Map.Entry<String, Double> e : itemAffinityState.entries()) {
                items.put(e.getKey(), e.getValue());
            }
        }
        if (categoryAffinityState != null) {
            for (Map.Entry<String, Double> e : categoryAffinityState.entries()) {
                cats.put(e.getKey(), e.getValue());
            }
        }
        Long ts = lastEventTsState == null ? null : lastEventTsState.value();
        Long clicks = clickCountState == null ? null : clickCountState.value();
        return new FeatureSnapshot(
                userId,
                cats,
                items,
                ts == null ? 0L : ts,
                clicks == null ? 0L : clicks);
    }

    private void writeSnapshot(FeatureSnapshot snap) throws Exception {
        itemAffinityState.clear();
        for (Map.Entry<String, Double> e : snap.itemAffinity().entrySet()) {
            itemAffinityState.put(e.getKey(), e.getValue());
        }
        categoryAffinityState.clear();
        for (Map.Entry<String, Double> e : snap.categoryAffinity().entrySet()) {
            categoryAffinityState.put(e.getKey(), e.getValue());
        }
        lastEventTsState.update(snap.lastEventTs());
        clickCountState.update(snap.clickCount());
    }
}
