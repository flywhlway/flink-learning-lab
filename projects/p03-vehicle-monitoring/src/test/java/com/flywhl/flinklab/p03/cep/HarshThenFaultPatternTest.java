package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wave 0 RED 夹具：锁定 HARSH_ACCEL/DTC 阈值与 within(30s) 语义。
 * 当前 {@link HarshThenFaultPattern#build()} 故意缺 within，本测试应失败。
 */
class HarshThenFaultPatternTest {

    @Test
    void harshAndFaultPredicatesAcceptValidEvents() throws Exception {
        Pattern<VehicleEvent, ?> pattern = HarshThenFaultPattern.build();

        VehicleEvent harsh = new VehicleEvent("VIN-1", "HARSH_ACCEL", 451.0, 1_000L);
        VehicleEvent fault = new VehicleEvent("VIN-1", "DTC", 481.0, 5_000L);

        assertTrue(matches(pattern.getCondition(), fault),
                "DTC value>480 应匹配 fault 谓词");
        assertTrue(matches(pattern.getPrevious().getCondition(), harsh),
                "HARSH_ACCEL value>450 应匹配 harsh 谓词");
    }

    @Test
    void patternRequiresWithinThirtySeconds() {
        Pattern<VehicleEvent, ?> pattern = HarshThenFaultPattern.build();
        Duration window = pattern.getWindowSize()
                .orElseThrow(() -> new AssertionError(
                        "Pattern 必须设置 within(Duration.ofSeconds(30))"));
        assertEquals(Duration.ofSeconds(30), window,
                "within 窗口必须为 30 秒（对齐 e10-C5）");
    }

    @SuppressWarnings("unchecked")
    private static boolean matches(
            IterativeCondition<? extends VehicleEvent> condition,
            VehicleEvent event) throws Exception {
        return ((IterativeCondition<VehicleEvent>) condition).filter(event, null);
    }
}
