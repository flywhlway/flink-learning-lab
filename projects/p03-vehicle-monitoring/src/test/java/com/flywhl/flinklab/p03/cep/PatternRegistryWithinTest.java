package com.flywhl.flinklab.p03.cep;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * D-11 / VEH-03：注册表恰好 3 条，且每条 Pattern 必须 within（getWindowSize 非空）。
 *
 * <p>Wave 0 RED：故意引用尚未交付的 {@link PatternRegistry}。
 */
class PatternRegistryWithinTest {

    @Test
    void everyRegisteredPatternRequiresWithin() {
        var all = PatternRegistry.all();
        assertEquals(3, all.size(), "模式库必须恰好 3 条（D-01）");

        Set<String> ids = new HashSet<>();
        for (var entry : all) {
            assertTrue(
                    entry.pattern().getWindowSize().isPresent(),
                    entry.id() + " 必须 within（docs/10-cep / D-11）");
            ids.add(entry.id());
        }

        assertEquals(
                Set.of(
                        PatternIds.HARSH_THEN_FAULT,
                        PatternIds.TRIPLE_HARSH,
                        PatternIds.DTC_PAIR),
                ids,
                "注册表 ID 集合必须恰好为 HARSH_THEN_FAULT / TRIPLE_HARSH / DTC_PAIR");
    }
}
