package com.flywhl.flinklab.p01;

/**
 * p01 作业参数集中解析：手写 {@code --key} / {@code --key=value}（D-04）。
 *
 * <p>默认 {@code --ai.enabled=false}；kafka/clickhouse 默认对齐 compose 网络；
 * AI 相关默认值供后续切片覆写，本切片不发起 HTTP。
 */
public final class JobConfig {

    public final String jobName;
    public final long checkpointIntervalMs;
    public final long checkpointTimeoutMs;
    public final String kafkaBootstrap;
    public final String eventsTopic;
    public final String groupId;
    public final String clickhouseUrl;
    public final String clickhouseUser;
    public final String clickhousePassword;
    public final String stateBackendType;
    public final boolean aiEnabled;
    public final String aiEndpoint;
    public final String aiModel;
    public final long aiTimeoutMs;
    public final int aiCapacity;
    public final int aiRetry;
    public final int budgetMaxAiCalls;
    public final String guardrailKeywords;

    private JobConfig(
            String jobName,
            long checkpointIntervalMs,
            long checkpointTimeoutMs,
            String kafkaBootstrap,
            String eventsTopic,
            String groupId,
            String clickhouseUrl,
            String clickhouseUser,
            String clickhousePassword,
            String stateBackendType,
            boolean aiEnabled,
            String aiEndpoint,
            String aiModel,
            long aiTimeoutMs,
            int aiCapacity,
            int aiRetry,
            int budgetMaxAiCalls,
            String guardrailKeywords) {
        this.jobName = jobName;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.checkpointTimeoutMs = checkpointTimeoutMs;
        this.kafkaBootstrap = kafkaBootstrap;
        this.eventsTopic = eventsTopic;
        this.groupId = groupId;
        this.clickhouseUrl = clickhouseUrl;
        this.clickhouseUser = clickhouseUser;
        this.clickhousePassword = clickhousePassword;
        this.stateBackendType = stateBackendType;
        this.aiEnabled = aiEnabled;
        this.aiEndpoint = aiEndpoint;
        this.aiModel = aiModel;
        this.aiTimeoutMs = aiTimeoutMs;
        this.aiCapacity = aiCapacity;
        this.aiRetry = aiRetry;
        this.budgetMaxAiCalls = budgetMaxAiCalls;
        this.guardrailKeywords = guardrailKeywords;
    }

    public static JobConfig from(String[] args) {
        return new JobConfig(
                arg(args, "job-name", "p01-log-ai"),
                Long.parseLong(arg(args, "checkpoint-interval-ms", "15000")),
                Long.parseLong(arg(args, "checkpoint-timeout-ms", "600000")),
                arg(args, "kafka-bootstrap", "kafka:9092"),
                arg(args, "events-topic", "logs.events"),
                arg(args, "group-id", "p01-log-ai"),
                arg(args, "clickhouse-url", "http://clickhouse:8123/"),
                arg(args, "clickhouse-user", "flinklab"),
                arg(args, "clickhouse-password", "flinklab123"),
                arg(args, "state-backend", "rocksdb"),
                Boolean.parseBoolean(arg(args, "ai.enabled", "false")),
                arg(args, "ai.endpoint", "http://host.docker.internal:11434"),
                arg(args, "ai.model", "qwen3:8b"),
                Long.parseLong(arg(args, "ai.timeout-ms", "8000")),
                Integer.parseInt(arg(args, "ai.capacity", "16")),
                Integer.parseInt(arg(args, "ai.retry", "2")),
                Integer.parseInt(arg(args, "budget.max-ai-calls", "120")),
                arg(args, "guardrail.keywords", ""));
    }

    /** 支持 {@code --key value} 与 {@code --key=value}。 */
    static String arg(String[] args, String key, String dflt) {
        String flag = "--" + key;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (flag.equals(a) && i + 1 < args.length) {
                return args[i + 1];
            }
            if (a.startsWith(flag + "=")) {
                return a.substring(flag.length() + 1);
            }
        }
        return dflt;
    }
}
