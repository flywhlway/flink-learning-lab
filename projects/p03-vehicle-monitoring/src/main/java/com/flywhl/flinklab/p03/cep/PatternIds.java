package com.flywhl.flinklab.p03.cep;

import java.util.Set;

/**
 * 模式库 patternId 单点常量（D-01 / D-11）。
 *
 * <p>仅允许三条预编译模式；Broadcast 控制面与 verify 白名单均对齐此处。
 */
public final class PatternIds {

    public static final String HARSH_THEN_FAULT = "HARSH_THEN_FAULT";
    public static final String TRIPLE_HARSH = "TRIPLE_HARSH";
    public static final String DTC_PAIR = "DTC_PAIR";

    private static final Set<String> KNOWN = Set.of(
            HARSH_THEN_FAULT, TRIPLE_HARSH, DTC_PAIR);

    private PatternIds() {
    }

    /** 是否为模式库白名单 ID。 */
    public static boolean isKnown(String patternId) {
        return patternId != null && KNOWN.contains(patternId);
    }
}
