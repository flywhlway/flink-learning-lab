package com.flywhl.flinklab.p01;

import com.flywhl.flinklab.p01.model.LogEvent;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 结构化日志 JSON 解析契约（D-09 / LOG-01/02）：必填字段产出；脏数据丢弃。
 *
 * <p>Wave 0 RED：引用尚未交付的 {@link ParseLogJson} / {@link LogEvent}，编译或断言失败以建立反馈环。
 */
class ParseLogJsonTest {

    @Test
    void parsesValidJsonIntoRequiredFields() {
        String json = """
                {"service":"auth-svc","level":"ERROR","message":"login failed","traceId":"t-1","eventTime":1710000000000}
                """;

        Optional<LogEvent> parsed = ParseLogJson.tryParse(json.trim());
        assertTrue(parsed.isPresent(), "合法 JSON 必须产出 LogEvent");
        LogEvent e = parsed.get();
        assertEquals("auth-svc", e.service());
        assertEquals("ERROR", e.level());
        assertEquals("login failed", e.message());
        assertEquals("t-1", e.traceId());
        assertEquals(1710000000000L, e.eventTime());
    }

    @Test
    void discardsMissingRequiredFields() {
        String missingService = """
                {"level":"INFO","message":"ok","traceId":"t-2","eventTime":1710000000001}
                """;
        assertFalse(
                ParseLogJson.tryParse(missingService.trim()).isPresent(),
                "缺 service 必须丢弃（D-09）");
    }

    @Test
    void discardsServiceWithQuoteOrBackslash() {
        // T-04-01：service 含引号/反斜杠视为注入风险字符，解析侧丢弃
        String quoted = """
                {"service":"auth\\"svc","level":"ERROR","message":"x","traceId":"t-3","eventTime":1710000000002}
                """;
        assertFalse(
                ParseLogJson.tryParse(quoted.trim()).isPresent(),
                "service 含引号必须丢弃");

        String backslash = """
                {"service":"auth\\\\svc","level":"ERROR","message":"x","traceId":"t-4","eventTime":1710000000003}
                """;
        assertFalse(
                ParseLogJson.tryParse(backslash.trim()).isPresent(),
                "service 含反斜杠必须丢弃");
    }
}
