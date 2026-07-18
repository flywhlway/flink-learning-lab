package com.flywhl.flinklab.p02.model;

/**
 * 用户行为事件（p02 解析输入 POJO）。
 *
 * <p>字段对齐 D-09：userId / itemId / eventType / eventTime。
 * 公开字段 + 无参构造满足 Flink POJO；访问器兼容 Wave 0 单测契约。
 */
public final class BehaviorEvent {

    public String userId;
    public String itemId;
    public String eventType;
    public long eventTime;

    public BehaviorEvent() {
    }

    public BehaviorEvent(String userId, String itemId, String eventType, long eventTime) {
        this.userId = userId;
        this.itemId = itemId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public String userId() {
        return userId;
    }

    public String itemId() {
        return itemId;
    }

    public String eventType() {
        return eventType;
    }

    public long eventTime() {
        return eventTime;
    }
}
