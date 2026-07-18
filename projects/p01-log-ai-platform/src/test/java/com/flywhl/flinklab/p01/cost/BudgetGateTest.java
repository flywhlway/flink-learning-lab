package com.flywhl.flinklab.p01.cost;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AI 调用预算熔断纯函数契约（D-12 / LOG-04）：未超限 allow；达上限 trip。
 *
 * <p>Wave 0 RED：引用尚未交付的 {@link BudgetGate}。
 */
class BudgetGateTest {

    @Test
    void allowsWhenBelowMaxAiCalls() {
        BudgetGate gate = new BudgetGate(10);
        assertTrue(gate.allow(0), "calls=0 未超限必须 allow");
        assertTrue(gate.allow(9), "calls=9 < max=10 必须 allow");
        assertFalse(gate.isTripped(9), "未达上限不得 trip");
    }

    @Test
    void tripsWhenReachingMaxAiCalls() {
        BudgetGate gate = new BudgetGate(10);
        assertFalse(gate.allow(10), "calls=maxAiCalls 必须拒绝新 AI 调用");
        assertTrue(gate.isTripped(10), "达到 maxAiCalls 必须 trip/熔断");
        assertTrue(gate.isTripped(11), "超限后保持熔断态");
    }
}
