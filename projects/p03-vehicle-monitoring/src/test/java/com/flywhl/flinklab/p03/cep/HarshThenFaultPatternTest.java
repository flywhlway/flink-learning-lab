package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.AlertEvent;
import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HarshThenFault 模式谓词 / within(30s) / Handler TIMEOUT 旁路契约。
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

    @Test
    void processTimedOutMatchEmitsTimeoutAlert() throws Exception {
        OutputTag<AlertEvent> tag = new OutputTag<>("test-timeout") {
        };
        AlertPatternHandler handler = new AlertPatternHandler(tag);
        CapturingContext ctx = new CapturingContext();

        VehicleEvent harsh = new VehicleEvent("VIN-T", "HARSH_ACCEL", 460.0, 10_000L);
        Map<String, List<VehicleEvent>> partial = new HashMap<>();
        partial.put("harsh", List.of(harsh));

        handler.processTimedOutMatch(partial, ctx);

        assertFalse(ctx.sideOutputs.isEmpty(),
                "超时半成品必须经 ctx.output 写出 Side Output");
        AlertEvent alert = ctx.sideOutputs.get(0);
        assertEquals("TIMEOUT", alert.alertType,
                "超时路径 alertType 必须为 TIMEOUT");
        assertEquals("VIN-T", alert.vin);
        assertEquals(460.0, alert.harshValue, 0.001);
    }

    @SuppressWarnings("unchecked")
    private static boolean matches(
            IterativeCondition<? extends VehicleEvent> condition,
            VehicleEvent event) throws Exception {
        return ((IterativeCondition<VehicleEvent>) condition).filter(event, null);
    }

    /** 捕获 Side Output 的测试用 Context 桩。 */
    static final class CapturingContext implements PatternProcessFunction.Context {
        final List<AlertEvent> sideOutputs = new ArrayList<>();

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            sideOutputs.add((AlertEvent) value);
        }

        @Override
        public long timestamp() {
            return 0L;
        }

        @Override
        public long currentProcessingTime() {
            return 0L;
        }
    }
}
