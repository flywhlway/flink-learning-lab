package com.flywhl.flinklab.p01.enrich;

import com.flywhl.flinklab.p01.model.LogEvent;
import com.flywhl.flinklab.p01.model.LogResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 按 service Keyed State 维护简易规则特征（近期 ERROR 计数）→ {@code featureJson}。
 *
 * <p>不依赖 Redis / Ollama（LOG-02 / RESEARCH A1）。
 */
public final class FeatureEnricher extends KeyedProcessFunction<String, LogEvent, LogResult> {

    private transient ValueState<Long> errorCountState;

    @Override
    public void open(OpenContext ctx) {
        errorCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("p01-error-count", Long.class));
    }

    @Override
    public void processElement(LogEvent event, Context ctx, Collector<LogResult> out)
            throws Exception {
        long prev = errorCountState.value() == null ? 0L : errorCountState.value();
        long next = prev;
        if (event.level != null && "ERROR".equalsIgnoreCase(event.level)) {
            next = prev + 1;
            errorCountState.update(next);
        }
        LogResult result = new LogResult(event);
        // 无引号紧凑格式：Sink 拒引号/反斜杠（T-04-01），避免合法 JSON 双引号被误杀
        result.featureJson = "{errorCount:" + next + ",service:"
                + (event.service == null ? "" : event.service) + "}";
        out.collect(result);
    }
}
