package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.PatternControlMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Broadcast 出口门控决策契约（D-05/D-06/D-07）：默认激活集、version 单调、按 patternId 过滤。
 *
 * <p>轻量单测：通过 {@link PatternActivationGate} 包内可见辅助方法断言，不必 MiniCluster。
 *
 * <p>Wave 0 RED：故意引用尚未交付的 Gate / ControlMessage / PatternIds。
 */
class PatternActivationGateTest {

    @Test
    void emptyStateDefaultsToHarshThenFaultOnly() {
        Set<String> active = PatternActivationGate.resolveActivePatterns(null);
        assertEquals(
                Set.of(PatternIds.HARSH_THEN_FAULT),
                active,
                "空 Broadcast State ≡ 仅激活 HARSH_THEN_FAULT（D-06）");
        assertTrue(
                PatternActivationGate.isAllowed(active, PatternIds.HARSH_THEN_FAULT),
                "默认集必须放行 HARSH_THEN_FAULT");
        assertFalse(
                PatternActivationGate.isAllowed(active, PatternIds.TRIPLE_HARSH),
                "默认集不得放行未激活的 TRIPLE_HARSH");
    }

    @Test
    void lowerVersionDoesNotOverrideHigher() {
        assertFalse(
                PatternActivationGate.isNewerVersion(5L, 3L),
                "stored version=5 时 incoming=3 不得覆盖（D-05 单调）");
        assertFalse(
                PatternActivationGate.isNewerVersion(5L, 5L),
                "相等 version 不得覆盖");
        assertTrue(
                PatternActivationGate.isNewerVersion(5L, 6L),
                "更高 version 必须允许更新");
    }

    @Test
    void filtersAlertWhenPatternIdNotActive() {
        PatternControlMessage msg = new PatternControlMessage();
        msg.activePatterns = List.of(PatternIds.HARSH_THEN_FAULT);
        msg.version = 1L;

        Set<String> active = PatternActivationGate.resolveActivePatterns(msg);
        assertFalse(
                PatternActivationGate.isAllowed(active, PatternIds.DTC_PAIR),
                "activePatterns 不含 DTC_PAIR 时该 AlertEvent 必须被丢弃（D-07/D-09）");
        assertTrue(
                PatternActivationGate.isAllowed(active, PatternIds.HARSH_THEN_FAULT),
                "激活集内的 patternId 必须放行");
    }
}
