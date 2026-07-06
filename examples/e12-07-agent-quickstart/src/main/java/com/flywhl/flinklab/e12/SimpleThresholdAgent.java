package com.flywhl.flinklab.e12;

import org.apache.flink.agents.api.Action;
import org.apache.flink.agents.api.Agent;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.context.RunnerContext;

/**
 * ai/第07章 Demo · 最小 Agent:阈值检测(不含 LLM,先跑通 Action+Event 骨架)。
 *
 * <p>核心机制验证点:
 * ① @Action(listenEvents) 声明事件监听 → AgentPlan 编译期反射提取路由表;
 * ② ctx.sendEvent 必须在 mailbox 线程调用(本方法体天然满足,未用 executeAsync);
 * ③ 无外呼、无状态,是理解 Agents 运行时的最小闭环。
 *
 * <p>⚠️ Preview API(0.3.0):方法签名可能随版本变更,报编译错时以
 * 官方 nightly 文档(Get Started → Quickstart)为准调整。
 */
public class SimpleThresholdAgent extends Agent {

    @Action(listenEvents = {InputEvent.class})
    public void checkThreshold(Event event, RunnerContext ctx) throws Exception {
        InputEvent in = (InputEvent) event;
        VehicleSignal signal = (VehicleSignal) in.getInput();

        if (signal.value > signal.threshold) {
            ctx.sendEvent(new OutputEvent(
                    "ALERT vin=%s signal=%s value=%.1f > threshold=%.1f".formatted(
                            signal.vin, signal.type, signal.value, signal.threshold)));
        }
    }
}
