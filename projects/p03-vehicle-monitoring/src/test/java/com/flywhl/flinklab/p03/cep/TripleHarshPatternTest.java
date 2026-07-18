package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TRIPLE_HARSH：times(3).consecutive() + within(20s)；谓词 HARSH_ACCEL value&gt;450（D-02）。
 *
 * <p>Wave 0 RED：故意引用尚未交付的 {@link TripleHarshPattern}。
 */
class TripleHarshPatternTest {

    @Test
    void patternRequiresWithinTwentySeconds() {
        Pattern<VehicleEvent, ?> pattern = TripleHarshPattern.build();
        Duration window = pattern.getWindowSize()
                .orElseThrow(() -> new AssertionError(
                        "TRIPLE_HARSH 必须设置 within(Duration.ofSeconds(20))"));
        assertEquals(Duration.ofSeconds(20), window,
                "within 窗口必须为 20 秒（D-02）");
    }

    @Test
    void harshPredicateAcceptsAccelAbove450() throws Exception {
        Pattern<VehicleEvent, ?> pattern = TripleHarshPattern.build();
        assertNotNull(pattern.getTimes(), "TRIPLE_HARSH 必须 times(3)");
        assertEquals(3, pattern.getTimes().getFrom(), "times 下界必须为 3");
        assertEquals(3, pattern.getTimes().getTo(), "times 上界必须为 3");

        VehicleEvent harsh = new VehicleEvent("VIN-1", "HARSH_ACCEL", 451.0, 1_000L);
        assertTrue(
                matches(pattern.getCondition(), harsh),
                "HARSH_ACCEL value>450 应匹配 TRIPLE_HARSH 谓词");
    }

    @SuppressWarnings("unchecked")
    private static boolean matches(
            IterativeCondition<? extends VehicleEvent> condition,
            VehicleEvent event) throws Exception {
        return ((IterativeCondition<VehicleEvent>) condition).filter(event, null);
    }
}
