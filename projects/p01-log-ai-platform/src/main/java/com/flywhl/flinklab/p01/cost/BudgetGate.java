package com.flywhl.flinklab.p01.cost;

/**
 * AI 调用预算熔断纯逻辑（D-12 / LOG-04）：供单测与 {@link BudgetGateFunction} 共用。
 *
 * <p>{@code calls} 为已放行（即将进入 Async）的累计次数；达到 {@code maxAiCalls} 即 trip。
 */
public final class BudgetGate {

    private final int maxAiCalls;

    public BudgetGate(int maxAiCalls) {
        this.maxAiCalls = Math.max(0, maxAiCalls);
    }

    /** 当前累计 calls 未达上限时可放行新 AI 调用。 */
    public boolean allow(int calls) {
        return calls < maxAiCalls;
    }

    /** 已达/超过上限则处于熔断态。 */
    public boolean isTripped(int calls) {
        return calls >= maxAiCalls;
    }

    public int maxAiCalls() {
        return maxAiCalls;
    }
}
