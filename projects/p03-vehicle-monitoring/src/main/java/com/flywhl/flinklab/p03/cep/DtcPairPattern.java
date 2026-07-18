package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;

/**
 * 重复故障：DTC followedBy DTC + within(15s) + skipPastLastEvent（D-02）。
 *
 * <p>skip 必须挂在 begin，禁止默认 {@code begin("dtc1")}（RESEARCH Pitfall 8）。
 */
public final class DtcPairPattern {

    private DtcPairPattern() {
    }

    /**
     * 构建模式：15s 内两次 DTC(value&gt;480)，匹配后跳过至末事件。
     */
    public static Pattern<VehicleEvent, ?> build() {
        return Pattern.<VehicleEvent>begin(
                        "dtc1", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(SimpleCondition.of(
                        e -> "DTC".equals(e.signalType) && e.value > 480))
                .followedBy("dtc2")
                .where(SimpleCondition.of(
                        e -> "DTC".equals(e.signalType) && e.value > 480))
                .within(Duration.ofSeconds(15));
    }
}
