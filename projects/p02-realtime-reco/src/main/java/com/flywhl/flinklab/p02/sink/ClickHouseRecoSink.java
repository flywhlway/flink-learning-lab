package com.flywhl.flinklab.p02.sink;

import com.flywhl.flinklab.p02.model.RecoResult;
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
import java.util.Locale;
import java.util.Set;

/**
 * ClickHouse HTTP SinkV2：写入 {@code flinklab.reco_results}（D-06/D-10 / T-05-01）。
 *
 * <p>表名列名常量；user_id/item_id/reason/feature_source 拒引号反斜杠；非 2xx 抛 {@link IOException}。
 */
public final class ClickHouseRecoSink implements Sink<RecoResult> {

    private static final Set<String> ALLOWED_FEATURE_SOURCES = Set.of("REDIS", "STATE_ONLY");

    private static final String TABLE = "flinklab.reco_results";
    private static final String COLUMNS =
            "(user_id,item_id,score,event_time,reason,feature_source,feature_snapshot)";

    private final String baseUrl;
    private final String user;
    private final String password;

    public ClickHouseRecoSink(String baseUrl, String user, String password) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.user = user;
        this.password = password;
    }

    @Override
    public SinkWriter<RecoResult> createWriter(WriterInitContext context) {
        return new BatchWriter(baseUrl, user, password);
    }

    static final class BatchWriter implements SinkWriter<RecoResult> {
        private final String baseUrl;
        private final String user;
        private final String password;
        private final List<RecoResult> buffer = new ArrayList<>();

        BatchWriter(String baseUrl, String user, String password) {
            this.baseUrl = baseUrl;
            this.user = user;
            this.password = password;
        }

        @Override
        public void write(RecoResult row, Context context) {
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
            for (RecoResult r : buffer) {
                String reason = r.reason == null ? "" : r.reason.replace('\'', ' ');
                String snap = r.featureSnapshot == null ? "{}" : r.featureSnapshot.replace('\'', ' ');
                body.append("('%s','%s',%s,fromUnixTimestamp64Milli(%d),'%s','%s','%s')"
                                .formatted(
                                        r.userId,
                                        r.itemId,
                                        formatScore(r.score),
                                        r.eventTimeMs,
                                        reason,
                                        r.featureSource,
                                        snap))
                        .append(',');
            }
            body.setLength(body.length() - 1);

            String sql = "INSERT INTO " + TABLE + COLUMNS + " VALUES " + body;
            String endpoint = baseUrl + "?user=" + user + "&password=" + password;
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

        static void validate(RecoResult row) {
            if (row == null
                    || row.userId == null
                    || row.itemId == null
                    || row.featureSource == null) {
                throw new IllegalArgumentException(
                        "RecoResult userId/itemId/featureSource 不能为空");
            }
            if (!ALLOWED_FEATURE_SOURCES.contains(row.featureSource)) {
                throw new IllegalArgumentException("非法 featureSource: " + row.featureSource);
            }
            if (containsForbidden(row.userId)
                    || containsForbidden(row.itemId)
                    || containsForbidden(row.featureSource)
                    || containsForbidden(row.reason == null ? "" : row.reason)
                    || containsForbidden(row.featureSnapshot == null ? "" : row.featureSnapshot)) {
                throw new IllegalArgumentException(
                        "RecoResult 字符串字段含引号或反斜杠，拒绝写入");
            }
        }

        private static boolean containsForbidden(String s) {
            return s.indexOf('"') >= 0
                    || s.indexOf('\'') >= 0
                    || s.indexOf('\\') >= 0;
        }

        private static String formatScore(double score) {
            return String.format(Locale.ROOT, "%.6f", score);
        }
    }
}
