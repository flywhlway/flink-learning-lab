package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ai/第16章 Demo · traceId 注入与跨算子传播（不依赖企业 APM）。
 *
 * <p>在入口把 userId+ts 合成 traceId，经两跳 map 始终携带同一 traceId，
 * 证明流作业可自建轻量追踪字段；生产再映射到 OTel baggage。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-16-trace-propagation \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.TracePropagationJob
 */
public final class TracePropagationJob {
    private TracePropagationJob() {
    }

    /** 携带 traceId 的业务事件包装。 */
    public static final class TracedEvent {
        public String traceId;
        public String userId;
        public String page;
        public long ts;

        public TracedEvent() {
        }

        public TracedEvent(String traceId, String userId, String page, long ts) {
            this.traceId = traceId;
            this.userId = userId;
            this.page = page;
            this.ts = ts;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TracedEvent> traced = Labs.events(env, "trace-src", 20, 4, 5, 200)
                .map(new InjectTrace())
                .uid("e12-16-inject");

        traced.map(new AnnotateHop("enrich"))
              .uid("e12-16-enrich")
              .map(new AnnotateHop("sink-prep"))
              .uid("e12-16-sink-prep")
              .process(new TraceLogger())
              .uid("e12-16-log")
              .print();

        env.execute("e12-16-trace-propagation");
    }

    public static final class InjectTrace implements MapFunction<Event, TracedEvent> {
        @Override
        public TracedEvent map(Event e) {
            String traceId = "tr-%s-%d".formatted(e.userId, e.ts);
            return new TracedEvent(traceId, e.userId, e.page, e.ts);
        }
    }

    /** 透传 TracedEvent，仅在内存中标记经过的 hop（通过 page 后缀不影响 traceId）。 */
    public static final class AnnotateHop implements MapFunction<TracedEvent, TracedEvent> {
        private final String hop;

        public AnnotateHop(String hop) {
            this.hop = hop;
        }

        @Override
        public TracedEvent map(TracedEvent te) {
            // 保持 traceId 不变；page 附加 hop 标记便于对照
            return new TracedEvent(te.traceId, te.userId, te.page + "@" + hop, te.ts);
        }
    }

    public static final class TraceLogger extends ProcessFunction<TracedEvent, String> {
        @Override
        public void processElement(TracedEvent te, Context ctx, Collector<String> out) {
            out.collect("TRACE  traceId=%s user=%s path=%s"
                    .formatted(te.traceId, te.userId, te.page));
        }
    }
}
