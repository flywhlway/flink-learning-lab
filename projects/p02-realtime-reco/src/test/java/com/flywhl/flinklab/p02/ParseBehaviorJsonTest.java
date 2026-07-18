package com.flywhl.flinklab.p02;

import com.flywhl.flinklab.p02.model.BehaviorEvent;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用户行为 JSON 解析契约（D-09 / RECO-01）：必填字段产出；脏数据丢弃。
 *
 * <p>Wave 0 RED：引用尚未交付的 {@link ParseBehaviorJson} / {@link BehaviorEvent}，
 * 编译或断言失败以建立反馈环。
 */
class ParseBehaviorJsonTest {

    @Test
    void parsesValidJsonIntoRequiredFields() {
        String json = """
                {"userId":"u-1","itemId":"i-10","eventType":"CLICK","eventTime":1710000000000}
                """;

        Optional<BehaviorEvent> parsed = ParseBehaviorJson.tryParse(json.trim());
        assertTrue(parsed.isPresent(), "合法 JSON 必须产出 BehaviorEvent");
        BehaviorEvent e = parsed.get();
        assertEquals("u-1", e.userId());
        assertEquals("i-10", e.itemId());
        assertEquals("CLICK", e.eventType());
        assertEquals(1710000000000L, e.eventTime());
    }

    @Test
    void discardsMissingRequiredFields() {
        String missingUserId = """
                {"itemId":"i-10","eventType":"VIEW","eventTime":1710000000001}
                """;
        assertFalse(
                ParseBehaviorJson.tryParse(missingUserId.trim()).isPresent(),
                "缺 userId 必须丢弃（D-09）");
    }

    @Test
    void discardsUserIdOrItemIdWithQuoteOrBackslash() {
        // T-05-01：userId/itemId 含引号/反斜杠视为注入风险字符，解析侧丢弃
        String quotedUser = """
                {"userId":"u\\"1","itemId":"i-10","eventType":"VIEW","eventTime":1710000000002}
                """;
        assertFalse(
                ParseBehaviorJson.tryParse(quotedUser.trim()).isPresent(),
                "userId 含引号必须丢弃");

        String backslashItem = """
                {"userId":"u-1","itemId":"i\\\\10","eventType":"VIEW","eventTime":1710000000003}
                """;
        assertFalse(
                ParseBehaviorJson.tryParse(backslashItem.trim()).isPresent(),
                "itemId 含反斜杠必须丢弃");
    }

    @Test
    void discardsUnknownEventType() {
        String badType = """
                {"userId":"u-1","itemId":"i-10","eventType":"WISH","eventTime":1710000000004}
                """;
        assertFalse(
                ParseBehaviorJson.tryParse(badType.trim()).isPresent(),
                "eventType 仅允许 VIEW|CLICK|CART|BUY");
    }

    @Test
    void acceptsAllAllowedEventTypes() {
        for (String type : new String[] {"VIEW", "CLICK", "CART", "BUY"}) {
            String json = "{\"userId\":\"u-1\",\"itemId\":\"i-10\",\"eventType\":\""
                    + type
                    + "\",\"eventTime\":1710000000005}";
            Optional<BehaviorEvent> parsed = ParseBehaviorJson.tryParse(json);
            assertTrue(parsed.isPresent(), "eventType=" + type + " 必须保留");
            assertEquals(type, parsed.get().eventType());
        }
    }
}
