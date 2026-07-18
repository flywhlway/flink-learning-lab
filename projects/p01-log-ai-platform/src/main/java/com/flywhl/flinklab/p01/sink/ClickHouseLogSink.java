package com.flywhl.flinklab.p01.sink;

import com.flywhl.flinklab.p01.model.LogResult;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * ClickHouse HTTP SinkV2：写入 {@code flinklab.log_results}（对齐 p03 / e07-C6）。
 *
 * <p>T-04-01：字符串字段拒引号与反斜杠；表名列名常量；非 2xx 抛 {@link IOException}。
 */
public final class ClickHouseLogSink implements Sink<LogResult> {

    private static final Set<String> ALLOWED_RULE_LABELS =
            Set.of("AUTH_FAIL", "ERROR_BURST", "NONE");
    private static final Set<String> ALLOWED_AI_RISKS =
            Set.of("HIGH", "MEDIUM", "LOW", "UNKNOWN", "NONE");
    private static final Set<String> ALLOWED_AI_SOURCES =
            Set.of("DISABLED", "AI", "RULE", "DEGRADED", "BLOCKED");

    private final String baseUrl;
    private final String user;
    private final String password;

    public ClickHouseLogSink(String baseUrl, String user, String password) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.user = user;
        this.password = password;
    }

    @Override
    public SinkWriter<LogResult> createWriter(WriterInitContext context) {
        return new BatchWriter(baseUrl, user, password);
    }

    static final class BatchWriter implements SinkWriter<LogResult> {
        private final String baseUrl;
        private final String user;
        private final String password;
        private final List<LogResult> buffer = new ArrayList<>();

        BatchWriter(String baseUrl, String user, String password) {
            this.baseUrl = baseUrl;
            this.user = user;
            this.password = password;
        }

        @Override
        public void write(LogResult row, Context context) {
            validate(row);
            buffer.add(row);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            if (buffer.isEmpty()) {
                return;
            }
            int batchSize = buffer.size();
            StringBuilder body = new StringBuilder();
            for (LogResult r : buffer) {
                String message = r.message == null ? "" : r.message.replace('\'', ' ');
                String feature = r.featureJson == null ? "{}" : r.featureJson.replace('\'', ' ');
                body.append("('%s','%s','%s','%s',fromUnixTimestamp64Milli(%d),'%s','%s','%s','%s')"
                                .formatted(
                                        r.service,
                                        r.level,
                                        message,
                                        r.traceId,
                                        r.eventTime,
                                        feature,
                                        r.ruleLabel,
                                        r.aiRisk,
                                        r.aiSource))
                        .append(',');
            }
            body.setLength(body.length() - 1);

            String sql = "INSERT INTO flinklab.log_results"
                    + "(service,level,message,trace_id,event_time,feature_json,rule_label,ai_risk,ai_source)"
                    + " VALUES "
                    + body;

            String endpoint = baseUrl
                    + "?user=" + user
                    + "&password=" + password;
            HttpURLConnection conn = (HttpURLConnection)
                    URI.create(endpoint).toURL().openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(30_000);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(sql.getBytes(StandardCharsets.UTF_8));
            }
            int code = conn.getResponseCode();
            buffer.clear();
            if (code < 200 || code >= 300) {
                throw new IOException(
                        "ClickHouse HTTP 非 2xx: status=" + code + " batchSize=" + batchSize);
            }
        }

        @Override
        public void close() {
        }

        static void validate(LogResult row) {
            if (row == null
                    || row.service == null
                    || row.level == null
                    || row.traceId == null
                    || row.ruleLabel == null
                    || row.aiRisk == null
                    || row.aiSource == null) {
                throw new IllegalArgumentException(
                        "LogResult service/level/traceId/ruleLabel/aiRisk/aiSource 不能为空");
            }
            if (!ALLOWED_RULE_LABELS.contains(row.ruleLabel)) {
                throw new IllegalArgumentException("非法 ruleLabel: " + row.ruleLabel);
            }
            if (!ALLOWED_AI_RISKS.contains(row.aiRisk)) {
                throw new IllegalArgumentException("非法 aiRisk: " + row.aiRisk);
            }
            if (!ALLOWED_AI_SOURCES.contains(row.aiSource)) {
                throw new IllegalArgumentException("非法 aiSource: " + row.aiSource);
            }
            if (containsForbidden(row.service)
                    || containsForbidden(row.level)
                    || containsForbidden(row.traceId)
                    || containsForbidden(row.ruleLabel)
                    || containsForbidden(row.aiRisk)
                    || containsForbidden(row.aiSource)
                    || containsForbidden(row.featureJson == null ? "" : row.featureJson)) {
                throw new IllegalArgumentException(
                        "LogResult 字符串字段含引号或反斜杠，拒绝写入");
            }
        }

        private static boolean containsForbidden(String s) {
            return s.indexOf('"') >= 0
                    || s.indexOf('\'') >= 0
                    || s.indexOf('\\') >= 0;
        }
    }
}
