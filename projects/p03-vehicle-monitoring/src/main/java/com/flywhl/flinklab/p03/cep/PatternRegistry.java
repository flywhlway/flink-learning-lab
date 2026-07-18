package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.cep.pattern.Pattern;

import java.util.List;

/**
 * 预编译 CEP 模式注册表：恰好 3 条工厂聚合，不解析 JSON、不接触 Kafka（D-01）。
 */
public final class PatternRegistry {

    private PatternRegistry() {
    }

    /**
     * 注册表条目：patternId ↔ 预编译 Pattern。
     */
    public record Entry(String id, Pattern<VehicleEvent, ?> pattern) {
    }

    /**
     * 返回恰好 3 条模式（顺序不限，集合固定）。
     */
    public static List<Entry> all() {
        return List.of(
                new Entry(PatternIds.HARSH_THEN_FAULT, HarshThenFaultPattern.build()),
                new Entry(PatternIds.TRIPLE_HARSH, TripleHarshPattern.build()),
                new Entry(PatternIds.DTC_PAIR, DtcPairPattern.build()));
    }
}
