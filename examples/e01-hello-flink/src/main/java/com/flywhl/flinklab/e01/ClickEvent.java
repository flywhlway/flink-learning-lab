package com.flywhl.flinklab.e01;

/**
 * 点击事件模型。
 *
 * <p>刻意使用「public 无参构造 + public 字段」的经典 POJO 形态,
 * 确保被 Flink 的 {@code PojoSerializer} 识别(性能与状态演进能力均优于 Kryo 兜底)。
 * Java record 自 Flink 1.19 起同样受支持,状态篇(e03)会对比两者的序列化行为。
 */
public class ClickEvent {

    public String userId;
    public String page;
    /** 事件时间,epoch millis(由客户端/生成器打点,而非服务端接收时间)。 */
    public long ts;

    public ClickEvent() {
    }

    public ClickEvent(String userId, String page, long ts) {
        this.userId = userId;
        this.page = page;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ClickEvent{userId='%s', page='%s', ts=%d}".formatted(userId, page, ts);
    }
}
