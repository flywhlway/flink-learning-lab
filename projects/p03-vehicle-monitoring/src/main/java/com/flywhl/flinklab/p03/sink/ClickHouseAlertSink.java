package com.flywhl.flinklab.p03.sink;

import com.flywhl.flinklab.p03.cep.PatternIds;
import com.flywhl.flinklab.p03.model.AlertEvent;
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
 * ClickHouse HTTP SinkV2：写入 {@code flinklab.vehicle_alerts}（对齐 e07-C6，修正 flush 坑）。
 *
 * <p>T-1-01 / T-02-06：vin / alertType / patternId 拒绝引号与反斜杠；表名列名常量；非 2xx 抛错。
 */
public final class ClickHouseAlertSink implements Sink<AlertEvent> {

    private static final Set<String> ALLOWED_ALERT_TYPES = Set.of("MATCH", "TIMEOUT");

    private final String baseUrl;
    private final String user;
    private final String password;

    public ClickHouseAlertSink(String baseUrl, String user, String password) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.user = user;
        this.password = password;
    }

    @Override
    public SinkWriter<AlertEvent> createWriter(WriterInitContext context) {
        return new BatchWriter(baseUrl, user, password);
    }

    static final class BatchWriter implements SinkWriter<AlertEvent> {
        private final String baseUrl;
        private final String user;
        private final String password;
        private final List<AlertEvent> buffer = new ArrayList<>();

        BatchWriter(String baseUrl, String user, String password) {
            this.baseUrl = baseUrl;
            this.user = user;
            this.password = password;
        }

        @Override
        public void write(AlertEvent alert, Context context) {
            validate(alert);
            buffer.add(alert);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            if (buffer.isEmpty()) {
                return;
            }
            int batchSize = buffer.size();
            StringBuilder body = new StringBuilder();
            for (AlertEvent a : buffer) {
                String summary = a.message == null ? "" : a.message.replace('\'', ' ');
                body.append("('%s','%s','%s','%s',%.4f,%.4f,fromUnixTimestamp64Milli(%d))"
                                .formatted(
                                        a.vin,
                                        a.alertType,
                                        a.patternId,
                                        summary,
                                        a.harshValue,
                                        a.faultValue,
                                        a.eventTime))
                        .append(',');
            }
            body.setLength(body.length() - 1);

            String sql = "INSERT INTO flinklab.vehicle_alerts"
                    + "(vin,alert_type,pattern_id,signal_summary,harsh_value,fault_value,event_time) VALUES "
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

        static void validate(AlertEvent alert) {
            if (alert == null || alert.vin == null || alert.alertType == null
                    || alert.patternId == null) {
                throw new IllegalArgumentException("AlertEvent vin/alertType/patternId 不能为空");
            }
            if (!ALLOWED_ALERT_TYPES.contains(alert.alertType)) {
                throw new IllegalArgumentException("非法 alertType: " + alert.alertType);
            }
            if (!PatternIds.isKnown(alert.patternId)) {
                throw new IllegalArgumentException("非法 patternId: " + alert.patternId);
            }
            if (containsForbidden(alert.vin)
                    || containsForbidden(alert.alertType)
                    || containsForbidden(alert.patternId)) {
                throw new IllegalArgumentException(
                        "vin/alertType/patternId 含引号或反斜杠，拒绝写入");
            }
        }

        private static boolean containsForbidden(String s) {
            return s.indexOf('"') >= 0
                    || s.indexOf('\'') >= 0
                    || s.indexOf('\\') >= 0;
        }
    }
}
