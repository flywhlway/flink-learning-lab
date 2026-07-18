package com.flywhl.flinklab.p02;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flywhl.flinklab.p02.model.BehaviorEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Kafka JSON → {@link BehaviorEvent}；非法/脏数据丢弃（D-09 / T-05-01）。
 *
 * <p>必填：userId / itemId / eventType / eventTime；
 * eventType 白名单 VIEW|CLICK|CART|BUY；userId/itemId 拒引号反斜杠，且字符集白名单。
 */
public final class ParseBehaviorJson extends RichFlatMapFunction<String, BehaviorEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ParseBehaviorJson.class);

    private static final Set<String> ALLOWED_EVENT_TYPES =
            Set.of("VIEW", "CLICK", "CART", "BUY");

    /** 阻断 Redis key / CH 注入：仅字母数字下划线连字符（T-05-01 / T-05-02）。 */
    private static final Pattern SAFE_ID = Pattern.compile("^[A-Za-z0-9_-]+$");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String json, Collector<BehaviorEvent> out) {
        tryParse(json).ifPresent(out::collect);
    }

    /**
     * 纯函数解析入口（单测 / 作业共用）；失败返回 empty，不抛。
     */
    public static Optional<BehaviorEvent> tryParse(String json) {
        if (json == null || json.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            String userId = textOrNull(node, "userId");
            String itemId = textOrNull(node, "itemId");
            String eventType = textOrNull(node, "eventType");
            if (userId == null || itemId == null || eventType == null) {
                return Optional.empty();
            }
            if (containsForbiddenChars(userId) || containsForbiddenChars(itemId)) {
                LOG.warn("丢弃含非法字符的 userId/itemId");
                return Optional.empty();
            }
            if (!SAFE_ID.matcher(userId).matches() || !SAFE_ID.matcher(itemId).matches()) {
                LOG.warn("丢弃字符集不合规的 userId/itemId");
                return Optional.empty();
            }
            if (!ALLOWED_EVENT_TYPES.contains(eventType)) {
                LOG.warn("丢弃非法 eventType={}", eventType);
                return Optional.empty();
            }
            JsonNode timeNode = node.get("eventTime");
            if (timeNode == null || !timeNode.canConvertToLong()) {
                return Optional.empty();
            }
            return Optional.of(new BehaviorEvent(
                    userId,
                    itemId,
                    eventType,
                    timeNode.asLong()));
        } catch (Exception e) {
            LOG.warn("解析 BehaviorEvent JSON 失败，丢弃: {}", e.toString());
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
