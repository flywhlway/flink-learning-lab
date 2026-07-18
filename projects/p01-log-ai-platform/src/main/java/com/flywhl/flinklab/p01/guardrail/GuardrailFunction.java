package com.flywhl.flinklab.p01.guardrail;

import com.flywhl.flinklab.p01.model.LogResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 输出侧关键词护栏（LOG-04 / D-12）：类比 e12-17 BLOCK 语义，本切片用静态
 * {@code JobConfig --guardrail.keywords}（非 Broadcast，减轻接线复杂度）。
 *
 * <p>命中 message / ai 相关文本 → {@code ai_source=BLOCKED}、截断敏感原文、
 * {@code guardrail_blocks++}；仍落库。禁止把模型输出当可信代码执行。
 */
public final class GuardrailFunction extends RichMapFunction<LogResult, LogResult> {

    private static final long serialVersionUID = 1L;
    private static final int REDACT_MAX_CHARS = 64;

    private final String keywordsCsv;

    private transient List<String> keywords;
    private transient Counter guardrailBlocks;

    public GuardrailFunction(String keywordsCsv) {
        this.keywordsCsv = keywordsCsv == null ? "" : keywordsCsv;
    }

    @Override
    public void open(OpenContext openContext) {
        keywords = parseKeywords(keywordsCsv);
        MetricGroup g = getRuntimeContext().getMetricGroup().addGroup("p01");
        guardrailBlocks = g.counter("guardrail_blocks");
    }

    @Override
    public LogResult map(LogResult value) {
        if (value == null) {
            return null;
        }
        if (keywords.isEmpty()) {
            return value;
        }
        String haystack = buildHaystack(value);
        String hit = firstMatch(haystack, keywords);
        if (hit == null) {
            return value;
        }
        guardrailBlocks.inc();
        LogResult blocked = copyOf(value);
        blocked.aiSource = "BLOCKED";
        blocked.message = redact(blocked.message);
        return blocked;
    }

    static List<String> parseKeywords(String csv) {
        List<String> out = new ArrayList<>();
        if (csv == null || csv.isBlank()) {
            return out;
        }
        for (String part : csv.split(",")) {
            String k = part.trim();
            if (!k.isEmpty()) {
                out.add(k);
            }
        }
        return out;
    }

    static String buildHaystack(LogResult value) {
        return nullToEmpty(value.message)
                + " "
                + nullToEmpty(value.aiRisk)
                + " "
                + nullToEmpty(value.aiSource)
                + " "
                + nullToEmpty(value.ruleLabel);
    }

    static String firstMatch(String haystack, List<String> keywords) {
        if (haystack == null || haystack.isBlank()) {
            return null;
        }
        for (String kw : keywords) {
            if (kw == null || kw.isBlank()) {
                continue;
            }
            // 词边界匹配：避免 "exfiltrate" 误伤 "exfiltrating"（verify-ai 造数）
            Pattern p = Pattern.compile(
                    "(?i)(?<!\\p{L})" + Pattern.quote(kw) + "(?!\\p{L})");
            if (p.matcher(haystack).find()) {
                return kw;
            }
        }
        return null;
    }

    static String redact(String message) {
        if (message == null || message.isBlank()) {
            return "[REDACTED]";
        }
        String trimmed = message.length() > REDACT_MAX_CHARS
                ? message.substring(0, REDACT_MAX_CHARS)
                : message;
        return "[REDACTED:" + trimmed + "]";
    }

    static LogResult copyOf(LogResult src) {
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
