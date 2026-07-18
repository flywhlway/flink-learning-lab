package com.flywhl.flinklab.p03;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flywhl.flinklab.p03.model.PatternControlMessage;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Kafka 控制面 JSON → {@link PatternControlMessage}；坏消息丢弃（T-02-01）。
 *
 * <p>契约：{@code {"activePatterns":["HARSH_THEN_FAULT"],"version":N}}（D-04）。
 */
public final class ParsePatternControlJson
        extends RichFlatMapFunction<String, PatternControlMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ParsePatternControlJson.class);

    private transient ObjectMapper mapper;

    @Override
    public void open(OpenContext openContext) {
        mapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String json, Collector<PatternControlMessage> out) {
        if (json == null || json.isBlank()) {
            return;
        }
        try {
            JsonNode node = mapper.readTree(json);
            JsonNode patternsNode = node.get("activePatterns");
            JsonNode versionNode = node.get("version");
            if (patternsNode == null || !patternsNode.isArray()
                    || versionNode == null || !versionNode.canConvertToLong()) {
                LOG.warn("丢弃非法控制消息：缺少 activePatterns/version");
                return;
            }
            List<String> patterns = new ArrayList<>();
            for (JsonNode p : patternsNode) {
                if (p != null && p.isTextual()) {
                    patterns.add(p.asText());
                }
            }
            PatternControlMessage msg = new PatternControlMessage();
            msg.activePatterns = patterns;
            msg.version = versionNode.asLong();
            out.collect(msg);
        } catch (Exception e) {
            LOG.warn("丢弃无法解析的控制消息: {}", e.toString());
        }
    }
}
