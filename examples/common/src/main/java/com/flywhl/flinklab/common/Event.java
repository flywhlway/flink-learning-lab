package com.flywhl.flinklab.common;

/** 全部实验共用的事件模型(POJO 形态,PojoSerializer 友好)。 */
public class Event {
    public String userId;
    public String page;
    public double amount;
    /** 事件时间 epoch millis。 */
    public long ts;

    public Event() {
    }

    public Event(String userId, String page, double amount, long ts) {
        this.userId = userId;
        this.page = page;
        this.amount = amount;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{u=%s, page=%s, amt=%.1f, ts=%d}".formatted(userId, page, amount, ts);
    }
}
