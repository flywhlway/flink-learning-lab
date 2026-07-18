package com.flywhl.flinklab.p03.sink;

import com.flywhl.flinklab.p03.window.WindowMetricsRow;
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

/**
 * ClickHouse HTTP SinkV2：写入 {@code flinklab.vehicle_window_metrics}。
 *
 * <p>T-03-04：表/列名常量；vin 拒引号与反斜杠；计数字段用数值格式化；非 2xx 抛错。
 */
public final class ClickHouseWindowMetricsSink implements Sink<WindowMetricsRow> {

    private final String baseUrl;
    private final String user;
    private final String password;

    public ClickHouseWindowMetricsSink(String baseUrl, String user, String password) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.user = user;
        this.password = password;
    }

    @Override
    public SinkWriter<WindowMetricsRow> createWriter(WriterInitContext context) {
        return new BatchWriter(baseUrl, user, password);
    }

    static final class BatchWriter implements SinkWriter<WindowMetricsRow> {
        private final String baseUrl;
        private final String user;
        private final String password;
        private final List<WindowMetricsRow> buffer = new ArrayList<>();

        BatchWriter(String baseUrl, String user, String password) {
            this.baseUrl = baseUrl;
            this.user = user;
            this.password = password;
        }

        @Override
        public void write(WindowMetricsRow row, Context context) {
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
            for (WindowMetricsRow r : buffer) {
                body.append("('%s',fromUnixTimestamp64Milli(%d),fromUnixTimestamp64Milli(%d),%d,%d,%d)"
                                .formatted(
                                        r.vin,
                                        r.windowStart,
                                        r.windowEnd,
                                        r.eventCount,
                                        r.harshCount,
                                        r.dtcCount))
                        .append(',');
            }
            body.setLength(body.length() - 1);

            String sql = "INSERT INTO flinklab.vehicle_window_metrics"
                    + "(vin,window_start,window_end,event_count,harsh_count,dtc_count) VALUES "
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

        static void validate(WindowMetricsRow row) {
            if (row == null || row.vin == null) {
                throw new IllegalArgumentException("WindowMetricsRow vin 不能为空");
            }
            if (containsForbidden(row.vin)) {
                throw new IllegalArgumentException("vin 含引号或反斜杠，拒绝写入");
            }
            if (row.eventCount < 0 || row.harshCount < 0 || row.dtcCount < 0) {
                throw new IllegalArgumentException("计数字段不能为负");
            }
        }

        static boolean containsForbidden(String s) {
            return s.indexOf('"') >= 0
                    || s.indexOf('\'') >= 0
                    || s.indexOf('\\') >= 0;
        }
    }
}
