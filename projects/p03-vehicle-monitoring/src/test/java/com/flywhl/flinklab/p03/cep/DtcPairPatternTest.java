package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipPastLastStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DTC_PAIR：DTC followedBy DTC + within(15s) + begin 挂 skipPastLastEvent（D-02）。
 *
 * <p>Wave 0 RED：故意引用尚未交付的 {@link DtcPairPattern}。
 */
class DtcPairPatternTest {

    @Test
    void patternRequiresWithinFifteenSeconds() {
        Pattern<VehicleEvent, ?> pattern = DtcPairPattern.build();
        Duration window = pattern.getWindowSize()
                .orElseThrow(() -> new AssertionError(
                        "DTC_PAIR 必须设置 within(Duration.ofSeconds(15))"));
        assertEquals(Duration.ofSeconds(15), window,
                "within 窗口必须为 15 秒（D-02）");
    }

    @Test
    void beginUsesSkipPastLastEvent() {
        Pattern<VehicleEvent, ?> pattern = DtcPairPattern.build();
        AfterMatchSkipStrategy skip = pattern.getAfterMatchSkipStrategy();
        assertInstanceOf(
                SkipPastLastStrategy.class,
                skip,
                "DTC_PAIR 必须在 begin 挂 AfterMatchSkipStrategy.skipPastLastEvent()");
    }

    @Test
    void dtcPredicatesAcceptValueAbove480() throws Exception {
        Pattern<VehicleEvent, ?> pattern = DtcPairPattern.build();
        VehicleEvent dtc = new VehicleEvent("VIN-1", "DTC", 481.0, 2_000L);

        assertTrue(
                matches(pattern.getCondition(), dtc),
                "DTC value>480 应匹配 dtc2 谓词");
        assertTrue(
                matches(pattern.getPrevious().getCondition(), dtc),
                "DTC value>480 应匹配 dtc1 谓词");
    }

    @SuppressWarnings("unchecked")
    private static boolean matches(
            IterativeCondition<? extends VehicleEvent> condition,
            VehicleEvent event) throws Exception {
        return ((IterativeCondition<VehicleEvent>) condition).filter(event, null);
    }
}
