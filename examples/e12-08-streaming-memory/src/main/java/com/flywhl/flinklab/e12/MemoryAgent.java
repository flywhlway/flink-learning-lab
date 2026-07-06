package com.flywhl.flinklab.e12;

import org.apache.flink.agents.api.Action;
import org.apache.flink.agents.api.Agent;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.context.RunnerContext;

/**
 * ai/第08章 Demo · 短期记忆(Short-Term Memory)读写:跨多次 Agent 运行的上下文保留。
 *
 * <p>验证点:同一 key(vin)的多条事件依次到达,Agent 通过
 * ctx.getShortTermMemory() 读到"上一次的信号值",输出携带前后对比——
 * 这是"记忆跨运行保留"的最小证明,底层即 Flink Keyed State 的托管封装
 * (ai/08 第 3 节:理解了 e03 ValueState 就理解了短期记忆)。
 *
 * <p>⚠️ Preview API(0.3.0):ShortTermMemory 的 get/set 签名以官方文档为准;
 * 0.3 新增的 TTL 配置(记忆过期)在集群部署配置中设置,本地最小示例未展开。
 */
public class MemoryAgent extends Agent {

    @Action(listenEvents = {InputEvent.class})
    public void trackSignalTrend(Event event, RunnerContext ctx) throws Exception {
        InputEvent in = (InputEvent) event;
        VehicleSignal signal = (VehicleSignal) in.getInput();

        // 读上一次的值(短期记忆,跨多次运行保留;首次为 null)
        Object prevObj = ctx.getShortTermMemory().get("last_value");
        Double prev = prevObj == null ? null : (Double) prevObj;

        String trend = prev == null ? "FIRST"
                : signal.value > prev ? "RISING"
                : signal.value < prev ? "FALLING" : "FLAT";

        // 写回记忆(mailbox 线程内,无并发问题)
        ctx.getShortTermMemory().set("last_value", signal.value);

        ctx.sendEvent(new OutputEvent(
                "vin=%s value=%.1f prev=%s trend=%s".formatted(
                        signal.vin, signal.value,
                        prev == null ? "-" : "%.1f".formatted(prev), trend)));
    }
}
