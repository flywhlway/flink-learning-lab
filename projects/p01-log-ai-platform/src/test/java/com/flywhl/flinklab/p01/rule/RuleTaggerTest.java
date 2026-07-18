package com.flywhl.flinklab.p01.rule;

import com.flywhl.flinklab.p01.model.LogEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 规则标签契约（LOG-02）：AUTH 类失败 → AUTH_FAIL；无关 INFO → NONE。
 *
 * <p>Wave 0 RED：引用尚未交付的 {@link RuleTagger} / {@link LogEvent}。
 */
class RuleTaggerTest {

    @Test
    void tagsAuthFailureAsAuthFail() {
        LogEvent authFail = new LogEvent(
                "auth-svc",
                "ERROR",
                "authentication failed for user alice",
                "trace-auth-1",
                1710000001000L);

        assertEquals(
                "AUTH_FAIL",
                RuleTagger.tag(authFail),
                "AUTH 类失败日志必须打 rule_label=AUTH_FAIL");
    }

    @Test
    void tagsIrrelevantInfoAsNone() {
        LogEvent info = new LogEvent(
                "billing-svc",
                "INFO",
                "invoice rendered",
                "trace-info-1",
                1710000002000L);

        assertEquals(
                "NONE",
                RuleTagger.tag(info),
                "无关 INFO 必须为 NONE（或等价空标签）");
    }
}
