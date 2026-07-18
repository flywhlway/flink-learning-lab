package com.flywhl.flinklab.p01;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flywhl.flinklab.p01.model.LogEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Kafka JSON → {@link LogEvent}；非法/脏数据丢弃（D-09 / T-04-01）。
 *
 * <p>必填：service / level / message / traceId / eventTime；
 * level 白名单 ERROR|WARN|INFO|DEBUG；service/traceId 拒引号与反斜杠。
 */
public final class ParseLogJson extends RichFlatMapFunction<String, LogEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ParseLogJson.class);

    private static final Set<String> ALLOWED_LEVELS =
            Set.of("ERROR", "WARN", "INFO", "DEBUG");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String json, Collector<LogEvent> out) {
        tryParse(json).ifPresent(out::collect);
    }

    /**
     * 纯函数解析入口（单测 / 作业共用）；失败返回 empty，不抛。
     */
    public static Optional<LogEvent> tryParse(String json) {
        if (json == null || json.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            String service = textOrNull(node, "service");
            String level = textOrNull(node, "level");
            String message = textOrNull(node, "message");
            String traceId = textOrNull(node, "traceId");
            if (service == null || level == null || message == null || traceId == null) {
                return Optional.empty();
            }
            if (containsForbiddenChars(service) || containsForbiddenChars(traceId)) {
                LOG.warn("丢弃含非法字符的 service/traceId");
                return Optional.empty();
            }
            if (!ALLOWED_LEVELS.contains(level)) {
                LOG.warn("丢弃非法 level={}", level);
                return Optional.empty();
            }
            JsonNode timeNode = node.get("eventTime");
            if (timeNode == null || !timeNode.canConvertToLong()) {
                return Optional.empty();
            }
            return Optional.of(new LogEvent(
                    service,
                    level,
                    message,
                    traceId,
                    timeNode.asLong()));
        } catch (Exception e) {
            LOG.warn("解析 LogEvent JSON 失败，丢弃: {}", e.toString());
            return Optional.empty();
        }
    }

    private static boolean containsForbiddenChars(String value) {
        return value.indexOf('"') >= 0 || value.indexOf('\\') >= 0;
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode n = node.get(field);
        if (n == null || n.isNull() || !n.isTextual()) {
            return null;
        }
        String v = n.asText();
        return v.isBlank() ? null : v;
    }
}
