package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * ai/第05章 Demo · Streaming RAG Lite：无 Milvus 的 Keyed State 片段索引 + 规则检索。
 *
 * <p>每个 userId 维护最近 N 条"文档片段"(用 page+amount 模拟 chunk)。
 * 当 amount 超过阈值时触发"查询"，在本地片段列表里做关键词/规则匹配并输出检索结果。
 * 证明：流式 RAG 的新鲜度敏感路径可以先用状态索引落地，向量库是增强而非起点。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-05-streaming-rag-lite \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingRagLiteJob
 */
public final class StreamingRagLiteJob {
    private StreamingRagLiteJob() {
    }

    private static final int MAX_CHUNKS = 8;
    private static final double QUERY_THRESHOLD = 400.0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> events = Labs.events(env, "rag-docs", 20, 4, 6, 300);

        events.keyBy(e -> e.userId)
              .process(new FragmentIndexRetriever())
              .uid("e12-05-rag-retrieve")
              .print();

        env.execute("e12-05-streaming-rag-lite");
    }

    /** 维护 per-user 片段窗口，高金额事件触发规则检索。 */
    public static final class FragmentIndexRetriever
            extends KeyedProcessFunction<String, Event, String> {
        private transient ListState<String> chunks;
        private transient ValueState<Integer> size;

        @Override
        public void open(OpenContext ctx) {
            chunks = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("chunks", String.class));
            size = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("chunk-size", Integer.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            String chunk = "page=%s amount=%.1f ts=%d".formatted(e.page, e.amount, e.ts);
            List<String> buf = new ArrayList<>();
            for (String c : chunks.get()) {
                buf.add(c);
            }
            buf.add(chunk);
            while (buf.size() > MAX_CHUNKS) {
                buf.remove(0);
            }
            chunks.update(buf);
            size.update(buf.size());

            if (e.amount < QUERY_THRESHOLD) {
                return;
            }
            String needle = e.page;
            List<String> hits = new ArrayList<>();
            for (String c : buf) {
                if (c.contains("page=" + needle)) {
                    hits.add(c);
                }
            }
            if (hits.isEmpty()) {
                out.collect("RAG-MISS  user=%s queryPage=%s indexSize=%d"
                        .formatted(e.userId, needle, buf.size()));
            } else {
                out.collect("RAG-HIT   user=%s queryPage=%s hits=%d top=%s"
                        .formatted(e.userId, needle, hits.size(), hits.get(hits.size() - 1)));
            }
        }
    }
}
