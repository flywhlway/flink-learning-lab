package com.flywhl.flinklab.p01.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flywhl.flinklab.p01.model.LogResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * Async I/O → 宿主机 Ollama {@code /api/chat} 日志风险分级（LOG-03 / D-01）。
 *
 * <p>强制 {@code stream=false}、{@code think=false}、{@code format=json}；
 * 解析 {@code risk∈HIGH|MEDIUM|LOW} → {@code ai_source=AI}；
 * 解析失败 → {@code ai_risk=UNKNOWN}/{@code ai_source=DEGRADED}；
 * {@link #timeout} 完成降级记录而非抛异常打挂作业。
 *
 * <p>禁止在 {@code asyncInvoke} 内阻塞 {@code Future.get()}（伪异步）。
 */
public final class OllamaRiskAsyncFunction extends RichAsyncFunction<LogResult, LogResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OllamaRiskAsyncFunction.class);

    private static final Set<String> RISK_ENUM = Set.of("HIGH", "MEDIUM", "LOW");
    private static final int MESSAGE_MAX_CHARS = 512;
    private static final String SYSTEM_PROMPT =
            "Classify log risk. Reply JSON {\"risk\":\"HIGH|MEDIUM|LOW\"} only.";

    private final String endpoint;
    private final String model;

    private transient HttpClient httpClient;
    private transient ObjectMapper mapper;
    private transient String chatUrl;
    private transient Counter aiCalls;
    private transient Counter aiTimeouts;
    private transient Counter aiDegrades;

    public OllamaRiskAsyncFunction(String endpoint, String model) {
        this.endpoint = endpoint == null || endpoint.isBlank()
                ? "http://host.docker.internal:11434"
                : endpoint.trim().replaceAll("/+$", "");
        this.model = model == null || model.isBlank() ? "qwen3:8b" : model.trim();
    }

    @Override
    public void open(OpenContext openContext) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        this.mapper = new ObjectMapper();
        this.chatUrl = endpoint + "/api/chat";
        // 实际调用侧指标（D-11 / RESEARCH Pattern 4）；budget_trips 在 BudgetGate
        MetricGroup g = getRuntimeContext().getMetricGroup().addGroup("p01");
        this.aiCalls = g.counter("ai_calls");
        this.aiTimeouts = g.counter("ai_timeouts");
        this.aiDegrades = g.counter("ai_degrades");
    }

    @Override
    public void asyncInvoke(LogResult input, ResultFuture<LogResult> resultFuture) {
        if (input == null) {
            resultFuture.complete(Collections.emptyList());
            return;
        }
        aiCalls.inc();
        final String body;
        try {
            body = buildRequestBody(input);
        } catch (Exception e) {
            aiDegrades.inc();
            resultFuture.complete(Collections.singleton(degraded(input)));
            return;
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(chatUrl))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete((response, err) -> {
                    if (err != null) {
                        // 交给 unorderedWaitWithRetry 重试策略
                        resultFuture.completeExceptionally(err);
                        return;
                    }
                    try {
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            resultFuture.completeExceptionally(new IllegalStateException(
                                    "Ollama HTTP " + response.statusCode()));
                            return;
                        }
                        LogResult parsed = parseChat(input, response.body());
                        if ("DEGRADED".equals(parsed.aiSource)) {
                            aiDegrades.inc();
                        }
                        resultFuture.complete(Collections.singleton(parsed));
                    } catch (Exception ex) {
                        LOG.warn("Ollama 响应解析失败，降级 DEGRADED: {}", ex.toString());
                        aiDegrades.inc();
                        resultFuture.complete(Collections.singleton(degraded(input)));
                    }
                });
    }

    @Override
    public void timeout(LogResult input, ResultFuture<LogResult> resultFuture) {
        // 总超时：降级落库，作业不因无模型/慢模型而失败（D-01）
        aiTimeouts.inc();
        aiDegrades.inc();
        resultFuture.complete(Collections.singleton(degraded(input)));
    }

    private String buildRequestBody(LogResult input) throws Exception {
        ObjectNode root = mapper.createObjectNode();
        root.put("model", model);
        root.put("stream", false);
        root.put("format", "json");
        root.put("think", false);

        ObjectNode options = root.putObject("options");
        options.put("temperature", 0);
        options.put("num_predict", 64);

        ArrayNode messages = root.putArray("messages");
        ObjectNode system = messages.addObject();
        system.put("role", "system");
        system.put("content", SYSTEM_PROMPT);

        ObjectNode user = messages.addObject();
        user.put("role", "user");
        user.put("content", buildUserContent(input));

        return mapper.writeValueAsString(root);
    }

    static String buildUserContent(LogResult input) {
        String msg = input.message == null ? "" : input.message;
        if (msg.length() > MESSAGE_MAX_CHARS) {
            msg = msg.substring(0, MESSAGE_MAX_CHARS);
        }
        return "level=" + nullToEmpty(input.level)
                + " service=" + nullToEmpty(input.service)
                + " message=" + msg;
    }

    private LogResult parseChat(LogResult input, String responseBody) throws Exception {
        JsonNode root = mapper.readTree(responseBody);
        String content = extractContent(root);
        if (content == null || content.isBlank()) {
            return degraded(input);
        }
        String risk = extractRisk(content);
        if (risk == null) {
            return degraded(input);
        }
        LogResult out = copyOf(input);
        out.aiRisk = risk;
        out.aiSource = "AI";
        return out;
    }

    private String extractContent(JsonNode root) {
        JsonNode message = root.path("message");
        if (message.isMissingNode()) {
            return null;
        }
        JsonNode content = message.path("content");
        if (content.isMissingNode() || content.isNull()) {
            return null;
        }
        return content.asText();
    }

    private String extractRisk(String content) throws Exception {
        String trimmed = content.trim();
        // content 可能是 JSON 对象，或被 markdown 包裹
        int start = trimmed.indexOf('{');
        int end = trimmed.lastIndexOf('}');
        if (start >= 0 && end > start) {
            trimmed = trimmed.substring(start, end + 1);
        }
        JsonNode node = mapper.readTree(trimmed);
        JsonNode riskNode = node.path("risk");
        if (riskNode.isMissingNode() || riskNode.isNull()) {
            return null;
        }
        String risk = riskNode.asText("").trim().toUpperCase(Locale.ROOT);
        if (!RISK_ENUM.contains(risk)) {
            return null;
        }
        return risk;
    }

    static LogResult degraded(LogResult input) {
        LogResult out = copyOf(input);
        out.aiRisk = "UNKNOWN";
        out.aiSource = "DEGRADED";
        return out;
    }

    static LogResult copyOf(LogResult src) {
        if (src == null) {
            return new LogResult();
        }
        return new LogResult(
                src.service,
                src.level,
                src.message,
                src.traceId,
                src.eventTime,
                src.featureJson,
                src.ruleLabel,
                src.aiRisk,
                src.aiSource);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
