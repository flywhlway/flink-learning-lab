package com.flywhl.flinklab.e07;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

import java.time.Duration;
import java.util.Map;

/**
 * e07-C4 · FileSink → MinIO(S3 协议):滚动策略三闸门。
 *
 * <p>DefaultRollingPolicy 的三个条件**任一满足**即滚动出新文件:
 * 文件大小达到上限 / 距上次滚动超过时间上限 / 文件打开后无新数据超过不活跃阈值。
 * 三闸门共同目标是"既不产生过多小文件(NameNode/元数据压力),
 * 也不让单文件长期不可见(下游批处理读不到新数据)"——
 * 这是数据湖(e09)入湖前必须理解的落盘物理层。
 *
 * <p>前置:MinIO 需要 s3 endpoint 配置(见 docker-compose 的 MINIO_* 环境变量与
 * flink-conf 里的 s3.endpoint/s3.access-key/s3.secret-key,已在 docker/ 配好)。
 *
 * <p>提交(集群):flink run -d -c 本类 e07-connectors jar;
 * 验证:mc ls local/warehouse/events/ 可见按分钟分桶的目录,文件随三闸门滚动。
 */
public final class C4FileSinkRollingPolicyJob {
    private C4FileSinkRollingPolicyJob() {
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = Configuration.fromMap(Map.of(
                "execution.checkpointing.dir", "file:///tmp/flink-lab/e07-c4-ckpt"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(20_000);   // FileSink 依赖 checkpoint 提交 part 文件(见 README)

        FileSink<String> sink = FileSink
                .forRowFormat(new Path("s3://warehouse/events"),
                        new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new org.apache.flink.streaming.api.functions.sink.filesystem.assigners
                        .DateTimeBucketAssigner<String>("yyyy-MM-dd--HH-mm"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(new org.apache.flink.core.fs.MemorySize(64L * 1024 * 1024))
                        .withRolloverInterval(Duration.ofMinutes(5))
                        .withInactivityInterval(Duration.ofMinutes(1))
                        .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("events").withPartSuffix(".jsonl").build())
                .build();

        Labs.events(env, "orders", 200, 30, 10, 500)
            .map(e -> "{\"user\":\"%s\",\"page\":\"%s\",\"amount\":%.2f,\"ts\":%d}"
                    .formatted(e.userId, e.page, e.amount, e.ts))
            .uid("e07-c4-fmt")
            .sinkTo(sink)
            .name("file-sink-minio").uid("e07-c4-sink");

        env.execute("e07-c4-file-sink-rolling-policy");
    }
}
