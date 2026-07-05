package com.flywhl.flinklab.template;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 作业参数集中解析点:所有可调参数从命令行/配置文件读入,严禁在业务代码里散落
 * 硬编码的 broker 地址、topic 名、checkpoint 间隔——这是"作业可移植性"的第一道纪律。
 *
 * <p>用法:flink run ... -- --job-name my-job --checkpoint-interval-ms 30000
 *          --kafka-bootstrap kafka:9092 --source-topic in --sink-topic out
 */
public final class JobConfig {

    public final String jobName;
    public final long checkpointIntervalMs;
    public final long checkpointTimeoutMs;
    public final String kafkaBootstrap;
    public final String sourceTopic;
    public final String sinkTopic;
    public final String stateBackendType;   // hashmap | rocksdb | forst

    private JobConfig(ParameterTool p) {
        this.jobName = p.get("job-name", "job-datastream-template");
        this.checkpointIntervalMs = p.getLong("checkpoint-interval-ms", 30_000);
        this.checkpointTimeoutMs = p.getLong("checkpoint-timeout-ms", 600_000);
        this.kafkaBootstrap = p.get("kafka-bootstrap", "kafka:9092");
        this.sourceTopic = p.get("source-topic", "events.raw");
        this.sinkTopic = p.get("sink-topic", "events.enriched");
        this.stateBackendType = p.get("state-backend", "rocksdb");
    }

    public static JobConfig from(String[] args) {
        return new JobConfig(ParameterTool.fromArgs(args));
    }
}
