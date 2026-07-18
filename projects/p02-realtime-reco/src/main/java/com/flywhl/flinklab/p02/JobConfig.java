package com.flywhl.flinklab.p02;

/**
 * p02 作业参数集中解析：手写 {@code --key} / {@code --key=value}（D-07）。
 *
 * <p>默认 kafka/redis/pg/clickhouse 对齐 compose 网络；{@code --top-k} 上限 50（T-05-04）。
 */
public final class JobConfig {

    /** Top-K 硬上限，防止恶意参数打爆打分（T-05-04）。 */
    public static final int TOP_K_MAX = 50;

    public final String jobName;
    public final long checkpointIntervalMs;
    public final long checkpointTimeoutMs;
    public final String kafkaBootstrap;
    public final String eventsTopic;
    public final String resultsTopic;
    public final String groupId;
    public final String clickhouseUrl;
    public final String clickhouseUser;
    public final String clickhousePassword;
    public final String redisHost;
    public final int redisPort;
    public final String postgresJdbcUrl;
    public final int topK;
    public final String stateBackendType;

    private JobConfig(
            String jobName,
            long checkpointIntervalMs,
            long checkpointTimeoutMs,
            String kafkaBootstrap,
            String eventsTopic,
            String resultsTopic,
            String groupId,
            String clickhouseUrl,
            String clickhouseUser,
            String clickhousePassword,
            String redisHost,
            int redisPort,
            String postgresJdbcUrl,
            int topK,
            String stateBackendType) {
        this.jobName = jobName;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.checkpointTimeoutMs = checkpointTimeoutMs;
        this.kafkaBootstrap = kafkaBootstrap;
        this.eventsTopic = eventsTopic;
        this.resultsTopic = resultsTopic;
        this.groupId = groupId;
        this.clickhouseUrl = clickhouseUrl;
        this.clickhouseUser = clickhouseUser;
        this.clickhousePassword = clickhousePassword;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.postgresJdbcUrl = postgresJdbcUrl;
        this.topK = topK;
        this.stateBackendType = stateBackendType;
    }

    public static JobConfig from(String[] args) {
        int topK = Integer.parseInt(arg(args, "top-k", "5"));
        if (topK < 1) {
            topK = 1;
        } else if (topK > TOP_K_MAX) {
            topK = TOP_K_MAX;
        }
        return new JobConfig(
                arg(args, "job-name", "p02-realtime-reco"),
                Long.parseLong(arg(args, "checkpoint-interval-ms", "15000")),
                Long.parseLong(arg(args, "checkpoint-timeout-ms", "600000")),
                arg(args, "kafka-bootstrap", "kafka:9092"),
                arg(args, "events-topic", "reco.events"),
                arg(args, "results-topic", "reco.results"),
                arg(args, "group-id", "p02-realtime-reco"),
                arg(args, "clickhouse-url", "http://clickhouse:8123/"),
                arg(args, "clickhouse-user", "flinklab"),
                arg(args, "clickhouse-password", "flinklab123"),
                arg(args, "redis-host", "redis"),
                Integer.parseInt(arg(args, "redis-port", "6379")),
                arg(args, "pg-jdbc", "jdbc:postgresql://postgres:5432/flinklab"),
                topK,
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
