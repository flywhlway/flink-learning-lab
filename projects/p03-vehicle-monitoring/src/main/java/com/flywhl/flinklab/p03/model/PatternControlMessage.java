package com.flywhl.flinklab.p03.model;

import java.util.List;

/**
 * Kafka 控制面消息模型（D-04）：确定性 JSON
 * {@code {"activePatterns":["HARSH_THEN_FAULT"],"version":N}}。
 *
 * <p>字段名与 JSON 对齐；无参构造供 Jackson / Flink POJO 使用。
 */
public final class PatternControlMessage {

    /** 待激活的 patternId 列表（须与 PatternIds 白名单求交）。 */
    public List<String> activePatterns;

    /** 单调递增版本号；仅更高 version 才写入 Broadcast State（D-05）。 */
    public long version;

    public PatternControlMessage() {
    }

    public PatternControlMessage(List<String> activePatterns, long version) {
        this.activePatterns = activePatterns;
        this.version = version;
    }
}
