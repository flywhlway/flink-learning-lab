package com.flywhl.flinklab.p03;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flywhl.flinklab.p03.model.VehicleEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Kafka JSON → {@link VehicleEvent}；非法/脏数据丢弃（T-1-01/02/03）。
 *
 * <p>白名单 signalType：HARSH_ACCEL | DTC | HEARTBEAT；vin 含引号则丢弃。
 */
public final class ParseVehicleJson extends RichFlatMapFunction<String, VehicleEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ParseVehicleJson.class);

    private static final Set<String> ALLOWED_SIGNAL_TYPES =
            Set.of("HARSH_ACCEL", "DTC", "HEARTBEAT");

    private transient ObjectMapper mapper;

    @Override
    public void open(OpenContext openContext) {
        mapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String json, Collector<VehicleEvent> out) {
        if (json == null || json.isBlank()) {
            return;
        }
        try {
            JsonNode node = mapper.readTree(json);
            String vin = textOrNull(node, "vin");
            String signalType = textOrNull(node, "signalType");
            if (vin == null || signalType == null) {
                return;
            }
            if (vin.indexOf('"') >= 0 || vin.indexOf('\\') >= 0) {
                LOG.warn("丢弃含非法字符的 vin");
                return;
            }
            if (!ALLOWED_SIGNAL_TYPES.contains(signalType)) {
                LOG.warn("丢弃非法 signalType={}", signalType);
                return;
            }
            JsonNode valueNode = node.get("value");
            JsonNode timeNode = node.get("eventTime");
            if (valueNode == null || !valueNode.isNumber()
                    || timeNode == null || !timeNode.canConvertToLong()) {
                return;
            }
            out.collect(new VehicleEvent(
                    vin,
                    signalType,
                    valueNode.asDouble(),
                    timeNode.asLong()));
        } catch (Exception e) {
            LOG.warn("解析 VehicleEvent JSON 失败，丢弃: {}", e.toString());
        }
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
