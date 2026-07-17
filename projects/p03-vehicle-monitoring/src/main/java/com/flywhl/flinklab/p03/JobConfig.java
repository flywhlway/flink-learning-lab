package com.flywhl.flinklab.p03;

/**
 * p03 作业参数集中解析：禁止在业务代码散落硬编码 broker/topic/checkpoint。
 *
 * <p>默认值对齐 compose 网络内服务名与 RESEARCH 约定。
 */
public final class JobConfig {

    public final String jobName;
    public final long checkpointIntervalMs;
    public final long checkpointTimeoutMs;
    public final String kafkaBootstrap;
    public final String eventsTopic;
    public final String alertsTopic;
    public final String groupId;
    public final String clickhouseUrl;
    public final String clickhouseUser;
    public final String clickhousePassword;
    public final String stateBackendType;

    private JobConfig(
            String jobName,
            long checkpointIntervalMs,
            long checkpointTimeoutMs,
            String kafkaBootstrap,
            String eventsTopic,
            String alertsTopic,
            String groupId,
            String clickhouseUrl,
            String clickhouseUser,
            String clickhousePassword,
            String stateBackendType) {
        this.jobName = jobName;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.checkpointTimeoutMs = checkpointTimeoutMs;
        this.kafkaBootstrap = kafkaBootstrap;
        this.eventsTopic = eventsTopic;
        this.alertsTopic = alertsTopic;
        this.groupId = groupId;
        this.clickhouseUrl = clickhouseUrl;
        this.clickhouseUser = clickhouseUser;
        this.clickhousePassword = clickhousePassword;
        this.stateBackendType = stateBackendType;
    }

    public static JobConfig from(String[] args) {
        return new JobConfig(
                arg(args, "job-name", "p03-vehicle-alert"),
                Long.parseLong(arg(args, "checkpoint-interval-ms", "15000")),
                Long.parseLong(arg(args, "checkpoint-timeout-ms", "600000")),
                arg(args, "kafka-bootstrap", "kafka:9092"),
                arg(args, "events-topic", "vehicle.events"),
                arg(args, "alerts-topic", "vehicle.alerts"),
                arg(args, "group-id", "p03-vehicle-alerts"),
                arg(args, "clickhouse-url", "http://clickhouse:8123/"),
                arg(args, "clickhouse-user", "flinklab"),
                arg(args, "clickhouse-password", "flinklab123"),
                arg(args, "state-backend", "rocksdb"));
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
