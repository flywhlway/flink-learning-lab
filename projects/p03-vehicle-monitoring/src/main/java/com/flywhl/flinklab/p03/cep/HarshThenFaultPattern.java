package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * 急加速后故障（harsh → fault）CEP 模式工厂。
 *
 * <p>Wave 0 故意省略 {@code within(30s)}，使 HarshThenFaultPatternTest 保持 RED；
 * 完整实现留给后续垂直切片（01-02）。
 */
public final class HarshThenFaultPattern {

    private HarshThenFaultPattern() {
    }

    /**
     * 构建模式：HARSH_ACCEL(value&gt;450) followedBy DTC(value&gt;480)。
     * Wave 0 缺 within —— 单测将断言失败。
     */
    public static Pattern<VehicleEvent, ?> build() {
        return Pattern.<VehicleEvent>begin("harsh")
                .where(SimpleCondition.of(
                        e -> "HARSH_ACCEL".equals(e.signalType) && e.value > 450))
                .followedBy("fault")
                .where(SimpleCondition.of(
                        e -> "DTC".equals(e.signalType) && e.value > 480));
        // Wave 0 RED：故意不调用 .within(Duration.ofSeconds(30))
    }
}
