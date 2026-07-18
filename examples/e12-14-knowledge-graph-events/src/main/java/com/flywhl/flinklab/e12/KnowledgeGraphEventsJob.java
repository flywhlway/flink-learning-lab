package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ai/第14章 Demo · 事件驱动知识图谱：三元组 upsert 进 MapState。
 *
 * <p>把 (user, VISITED, page) 写成边；同边重复到达时更新权重(amount)。
 * 证明 KG 增量维护可以是流式状态问题，不必先上图数据库。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-14-knowledge-graph-events \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.KnowledgeGraphEventsJob
 */
public final class KnowledgeGraphEventsJob {
    private KnowledgeGraphEventsJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "kg", 18, 4, 7, 250)
            .keyBy(e -> e.userId)
            .process(new TripleUpsert())
            .uid("e12-14-kg-upsert")
            .print();

        env.execute("e12-14-knowledge-graph-events");
    }

    public static final class TripleUpsert extends KeyedProcessFunction<String, Event, String> {
        private transient MapState<String, Double> edges;

        @Override
        public void open(OpenContext ctx) {
            edges = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("triples", String.class, Double.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            String predicate = "VISITED:" + e.page;
            Double prev = edges.get(predicate);
            double next = (prev == null ? 0.0 : prev) + e.amount;
            edges.put(predicate, next);
            String verb = prev == null ? "INSERT" : "UPDATE";
            out.collect("%s  (%s)-[VISITED]->(%s) weight=%.1f"
                    .formatted(verb, e.userId, e.page, next));
        }
    }
}
