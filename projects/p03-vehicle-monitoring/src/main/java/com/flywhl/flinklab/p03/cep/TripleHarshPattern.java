package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;

/**
 * 急加速突发：20s 内连续 3 次 HARSH_ACCEL(value&gt;450)（D-02；对齐 e10-C1）。
 *
 * <p>times(3).consecutive() 要求严格紧邻，中间夹其它事件即作废。
 */
public final class TripleHarshPattern {

    private TripleHarshPattern() {
    }

    /**
     * 构建模式：连续三次急加速，窗口 within(20s)。
     */
    public static Pattern<VehicleEvent, ?> build() {
        return Pattern.<VehicleEvent>begin("harsh")
                .where(SimpleCondition.of(
                        e -> "HARSH_ACCEL".equals(e.signalType) && e.value > 450))
                .times(3).consecutive()
                .within(Duration.ofSeconds(20));
    }
}
