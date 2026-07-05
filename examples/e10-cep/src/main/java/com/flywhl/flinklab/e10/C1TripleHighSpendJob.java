package com.flywhl.flinklab.e10;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * e10-C1 · CEP 第一课:同一用户 20s 内连续 3 笔高额(>400)消费 → 风险告警。
 *
 * <p>要点:① CEP 在 keyBy 之后按 key 独立匹配;② times(3).consecutive()
 * 要求严格连续(中间夹一笔低额即作废,去掉 consecutive 则允许穿插);
 * ③ within 限定整个模式的时间预算,超时的半成品匹配被丢弃(C3 教你接住它们)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e10-cep \
 *          -Dexec.mainClass=com.flywhl.flinklab.e10.C1TripleHighSpendJob
 */
public final class C1TripleHighSpendJob {
    private C1TripleHighSpendJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var keyed = Labs.events(env, "spend", 30, 4, 10, 1_000)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)))
                .keyBy(e -> e.userId);

        Pattern<Event, ?> pattern = Pattern.<Event>begin("high")
                .where(SimpleCondition.of(e -> e.amount > 400))
                .times(3).consecutive()
                .within(Duration.ofSeconds(20));

        CEP.pattern(keyed, pattern)
           .process(new PatternProcessFunction<Event, String>() {
               @Override
               public void processMatch(Map<String, List<Event>> match, Context ctx,
                                        Collector<String> out) {
                   List<Event> hits = match.get("high");
                   out.collect("RISK user=%s 连续3笔高额 amounts=[%.0f, %.0f, %.0f]"
                           .formatted(hits.get(0).userId,
                                   hits.get(0).amount, hits.get(1).amount, hits.get(2).amount));
               }
           })
           .uid("e10-c1-cep")
           .print();

        env.execute("e10-c1-triple-high-spend");
    }
}
