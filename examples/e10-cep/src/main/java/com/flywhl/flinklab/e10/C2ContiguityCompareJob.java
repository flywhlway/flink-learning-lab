package com.flywhl.flinklab.e10;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * e10-C2 · 连接语义对照:next(严格紧邻) vs followedBy(允许穿插)。
 *
 * <p>同一"浏览 /item → 支付 /pay"意图,两种连接同时匹配同一流:
 * NEXT 只在 /item 后**紧跟** /pay 时命中(命中少);FOLLOWED 允许中间逛别的页
 * (命中多)。还有第三种 followedByAny(连历史已匹配的也不放过,组合爆炸,慎用)。
 * 转化漏斗类需求 99% 是 followedBy。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e10-cep \
 *          -Dexec.mainClass=com.flywhl.flinklab.e10.C2ContiguityCompareJob
 */
public final class C2ContiguityCompareJob {
    private C2ContiguityCompareJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Event, String> keyed =
                Labs.events(env, "journey", 30, 4, 10, 1_000)
                    .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)))
                    .keyBy(e -> e.userId);

        attach(keyed, true,  "NEXT    ");
        attach(keyed, false, "FOLLOWED");
        env.execute("e10-c2-contiguity-compare");
    }

    private static void attach(KeyedStream<Event, String> keyed, boolean strict, String tag) {
        Pattern<Event, ?> browse = Pattern.<Event>begin("browse")
                .where(SimpleCondition.of(e -> "/item".equals(e.page)));
        Pattern<Event, ?> pattern = (strict
                ? browse.next("pay")
                : browse.followedBy("pay"))
                .where(SimpleCondition.of(e -> "/pay".equals(e.page)))
                .within(Duration.ofSeconds(15));

        CEP.pattern(keyed, pattern)
           .process(new PatternProcessFunction<Event, String>() {
               @Override
               public void processMatch(Map<String, List<Event>> m, Context ctx,
                                        Collector<String> out) {
                   out.collect("[%s] user=%s item→pay 转化".formatted(
                           tag, m.get("pay").get(0).userId));
               }
           })
           .uid("e10-c2-" + tag.trim())
           .print();
    }
}
