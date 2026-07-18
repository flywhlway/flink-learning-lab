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
 * e10-C7 · 低额后紧跟高额尖刺：next + 阈值条件，教学「突变」模式。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e10-cep \
 *          -Dexec.mainClass=com.flywhl.flinklab.e10.C7LowThenHighSpikeJob
 */
public final class C7LowThenHighSpikeJob {
    private C7LowThenHighSpikeJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var keyed = Labs.events(env, "cep7", 30, 4, 10, 800)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)))
                .keyBy(e -> e.userId);

        Pattern<Event, ?> pattern = Pattern.<Event>begin("low")
                .where(SimpleCondition.of(e -> e.amount < 100))
                .next("high")
                .where(SimpleCondition.of(e -> e.amount > 400))
                .within(Duration.ofSeconds(10));

        CEP.pattern(keyed, pattern)
           .process(new PatternProcessFunction<Event, String>() {
               @Override
               public void processMatch(Map<String, List<Event>> match, Context ctx,
                                        Collector<String> out) {
                   Event low = match.get("low").get(0);
                   Event high = match.get("high").get(0);
                   out.collect("SPIKE user=%s low=%.0f high=%.0f"
                           .formatted(low.userId, low.amount, high.amount));
               }
           })
           .uid("e10-c7-spike")
           .print();

        env.execute("e10-c7-low-then-high-spike");
    }
}
