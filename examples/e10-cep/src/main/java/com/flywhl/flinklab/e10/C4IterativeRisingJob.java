package com.flywhl.flinklab.e10;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * e10-C4 · 迭代条件:同一用户金额"三连涨"(每笔都高于上一笔)。
 *
 * <p>IterativeCondition 可以在判定当前事件时**回看本模式已捕获的事件**
 * (ctx.getEventsForPattern)——SimpleCondition 做不到的"相对条件"全靠它:
 * 连涨/连跌、比首笔高 X%、与均值偏离 N 倍……代价:每次判定要读回已匹配序列,
 * oneOrMore+复杂迭代条件是 CEP 性能事故的高发组合(README 展开)。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e10-cep \
 *          -Dexec.mainClass=com.flywhl.flinklab.e10.C4IterativeRisingJob
 */
public final class C4IterativeRisingJob {
    private C4IterativeRisingJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var keyed = Labs.events(env, "trades", 30, 3, 12, 1_000)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)))
                .keyBy(e -> e.userId);

        Pattern<Event, ?> pattern = Pattern.<Event>begin("first")
                .followedBy("second")
                .where(new HigherThanPrev("first"))
                .followedBy("third")
                .where(new HigherThanPrev("second"))
                .within(Duration.ofSeconds(30));

        CEP.pattern(keyed, pattern)
           .process(new PatternProcessFunction<Event, String>() {
               @Override
               public void processMatch(Map<String, List<Event>> m, Context ctx,
                                        Collector<String> out) {
                   out.collect("RISING user=%s %.0f → %.0f → %.0f".formatted(
                           m.get("first").get(0).userId,
                           m.get("first").get(0).amount,
                           m.get("second").get(0).amount,
                           m.get("third").get(0).amount));
               }
           })
           .uid("e10-c4-rising")
           .print();

        env.execute("e10-c4-iterative-rising");
    }

    /** 当前事件金额须高于指定前置阶段捕获的事件。 */
    public static final class HigherThanPrev extends IterativeCondition<Event> {
        private final String prevStage;

        public HigherThanPrev(String prevStage) {
            this.prevStage = prevStage;
        }

        @Override
        public boolean filter(Event current, Context<Event> ctx) throws Exception {
            for (Event prev : ctx.getEventsForPattern(prevStage)) {
                return current.amount > prev.amount;   // 该阶段只捕获一个事件
            }
            return false;
        }
    }
}
