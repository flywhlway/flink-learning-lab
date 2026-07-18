package com.flywhl.flinklab.p01.model;

/**
 * 结构化日志事件（p01 解析输入 POJO）。
 *
 * <p>字段对齐 D-09：service / level / message / traceId / eventTime。
 * 公开字段 + 无参构造满足 Flink POJO；访问器兼容 Wave 0 单测契约。
 */
public final class LogEvent {

    public String service;
    public String level;
    public String message;
    public String traceId;
    public long eventTime;

    public LogEvent() {
    }

    public LogEvent(String service, String level, String message, String traceId, long eventTime) {
        this.service = service;
        this.level = level;
        this.message = message;
        this.traceId = traceId;
        this.eventTime = eventTime;
    }

    public String service() {
        return service;
    }

    public String level() {
        return level;
    }

    public String message() {
        return message;
    }

    public String traceId() {
        return traceId;
    }

    public long eventTime() {
        return eventTime;
    }
}
