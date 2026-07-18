package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.AlertEvent;
import com.flywhl.flinklab.p03.model.PatternControlMessage;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 出口门控：非 keyed {@link BroadcastProcessFunction}（RESEARCH RESOLVED Q1 / A1）。
 *
 * <p>纪律对齐 e03-C7 / D-05：仅 {@link #processBroadcastElement} 写 Broadcast State；
 * {@link #processElement} 只读。内容完全来自控制消息（禁止本地时钟 / 随机）。
 *
 * <p>空状态 ≡ 默认激活 {@link PatternIds#HARSH_THEN_FAULT}（D-06）。
 */
public final class PatternActivationGate
        extends BroadcastProcessFunction<AlertEvent, PatternControlMessage, AlertEvent> {

    static final String STATE_KEY = "active";
    static final int MAX_ACTIVE_PATTERNS = 3;
    static final Set<String> DEFAULT_ACTIVE = Set.of(PatternIds.HARSH_THEN_FAULT);

    /** 建议 descriptor 名：p03-active-patterns（作业接线在 02-02b）。 */
    public static final MapStateDescriptor<String, PatternControlMessage> ACTIVE_DESC =
            new MapStateDescriptor<>("p03-active-patterns", String.class, PatternControlMessage.class);

    private final MapStateDescriptor<String, PatternControlMessage> descriptor;

    public PatternActivationGate() {
        this(ACTIVE_DESC);
    }

    public PatternActivationGate(MapStateDescriptor<String, PatternControlMessage> descriptor) {
        this.descriptor = descriptor;
    }

    /**
     * 解析有效激活集：null / 空列表 ≡ {@link #DEFAULT_ACTIVE}（D-06）。
     *
     * <p>包内可见，供单测不启 MiniCluster 断言决策。
     */
    static Set<String> resolveActivePatterns(PatternControlMessage msg) {
        if (msg == null || msg.activePatterns == null || msg.activePatterns.isEmpty()) {
            return DEFAULT_ACTIVE;
        }
        Set<String> sanitized = sanitizeActivePatterns(msg.activePatterns);
        if (sanitized.isEmpty()) {
            return DEFAULT_ACTIVE;
        }
        return Collections.unmodifiableSet(sanitized);
    }

    /** {@code incoming > stored} 才允许覆盖（D-05 version 单调）。 */
    static boolean isNewerVersion(long storedVersion, long incomingVersion) {
        return incomingVersion > storedVersion;
    }

    /** patternId 是否在当前激活集中（MATCH / TIMEOUT 同逻辑，D-09）。 */
    static boolean isAllowed(Set<String> active, String patternId) {
        return patternId != null && active != null && active.contains(patternId);
    }

    /**
     * 与 {@link PatternIds} 白名单求交，去重并截断至 ≤3（ASVS V5 / T-02-01）。
     * 未知 ID 忽略；超长列表截断。
     */
    static Set<String> sanitizeActivePatterns(List<String> raw) {
        if (raw == null) {
            return Set.of();
        }
        LinkedHashSet<String> out = new LinkedHashSet<>();
        for (String id : raw) {
            if (PatternIds.isKnown(id)) {
                out.add(id);
                if (out.size() >= MAX_ACTIVE_PATTERNS) {
                    break;
                }
            }
        }
        return out;
    }

    @Override
    public void processBroadcastElement(
            PatternControlMessage msg, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (msg == null || msg.activePatterns == null) {
            return;
        }
        // 超长原始列表视为坏消息，整条跳过（不写 state）
        if (msg.activePatterns.size() > MAX_ACTIVE_PATTERNS) {
            return;
        }
        Set<String> sanitized = sanitizeActivePatterns(msg.activePatterns);
        // 求交后为空且原列表非空 → 全是未知 ID，跳过
        if (sanitized.isEmpty() && !msg.activePatterns.isEmpty()) {
            return;
        }

        var state = ctx.getBroadcastState(descriptor);
        PatternControlMessage cur = state.get(STATE_KEY);
        long storedVersion = cur == null ? -1L : cur.version;
        if (!isNewerVersion(storedVersion, msg.version)) {
            return;
        }

        PatternControlMessage toStore = new PatternControlMessage();
        toStore.version = msg.version;
        toStore.activePatterns = new ArrayList<>(sanitized);
        state.put(STATE_KEY, toStore);
    }

    @Override
    public void processElement(AlertEvent alert, ReadOnlyContext ctx, Collector<AlertEvent> out)
            throws Exception {
        if (alert == null) {
            return;
        }
        ReadOnlyBroadcastState<String, PatternControlMessage> state =
                ctx.getBroadcastState(descriptor);
        PatternControlMessage cur = state.get(STATE_KEY);
        Set<String> active = resolveActivePatterns(cur);
        if (isAllowed(active, alert.patternId)) {
            out.collect(alert);
        }
    }
}
