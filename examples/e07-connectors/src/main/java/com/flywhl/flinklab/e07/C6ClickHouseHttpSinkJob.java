package com.flywhl.flinklab.e07;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * e07-C6 · 自定义 SinkV2:ClickHouse HTTP 接口批写(攒批 + checkpoint 时强制 flush)。
 *
 * <p>SinkV2 极简接口:Sink.createWriter 装配 SinkWriter;write() 攒批,
 * flush(boolean endOfInput) 在 **每次 checkpoint 前**被调用一次 —— 这是它与
 * e03-C5(CheckpointedFunction 手工攒批)的关系:SinkV2 把"何时该吐"的时机
 * 显式暴露成接口方法,不用再自己实现 snapshotState。
 * 语义等级:本例是 at-least-once(flush 在 checkpoint 前但非 2PC);
 * 真正 exactly-once 需要实现 TwoPhaseCommittingSink(参见 e04-C2 的 Kafka 范式)。
 *
 * <p>前置:docker compose exec clickhouse clickhouse-client --query
 *   "CREATE TABLE IF NOT EXISTS flinklab.raw_events(user_id String, page String,
 *    amount Float64, ts Int64) ENGINE=MergeTree ORDER BY ts"
 *
 * <p>提交(集群):flink run -d -c 本类 e07-connectors jar;
 * 验证:clickhouse-client 查询 count() 随 checkpoint 节拍增长。
 */
public final class C6ClickHouseHttpSinkJob {
    private C6ClickHouseHttpSinkJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(15_000);

        Labs.events(env, "orders", 100, 20, 10, 500)
            .sinkTo(new ClickHouseHttpSink())
            .name("clickhouse-http-sink").uid("e07-c6-sink");

        env.execute("e07-c6-clickhouse-http-sink");
    }

    /** 极简 SinkV2:每 subtask 一个 writer,内部攒批 + HTTP INSERT。 */
    public static final class ClickHouseHttpSink implements Sink<Event> {
        @Override
        public SinkWriter<Event> createWriter(InitContext ctx) {
            return new BatchWriter();
        }
    }

    static final class BatchWriter implements SinkWriter<Event> {
        private final List<Event> buffer = new ArrayList<>();

        BatchWriter() {
        }

        @Override
        public void write(Event e, Context ctx) {
            buffer.add(e);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            if (buffer.isEmpty()) {
                return;
            }
            StringBuilder body = new StringBuilder();
            for (Event e : buffer) {
                body.append("('%s','%s',%.2f,%d)".formatted(e.userId, e.page, e.amount, e.ts))
                    .append(',');
            }
            body.setLength(body.length() - 1);   // 去掉末尾逗号

            String sql = "INSERT INTO flinklab.raw_events(user_id,page,amount,ts) VALUES "
                    + body;
            HttpURLConnection conn = (HttpURLConnection)
                    URI.create("http://clickhouse:8123/").toURL().openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            try (OutputStream os = conn.getOutputStream()) {
                os.write(sql.getBytes(StandardCharsets.UTF_8));
            }
            int code = conn.getResponseCode();   // 生产版应检查非 200 并重试/告警
            buffer.clear();
            System.out.println("flush batch=%d httpStatus=%d".formatted(buffer.size(), code));
        }

        @Override
        public void close() {
        }
    }
}
