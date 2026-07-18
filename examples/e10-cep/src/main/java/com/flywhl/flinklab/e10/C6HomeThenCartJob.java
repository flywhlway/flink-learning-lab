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
 * e10-C6 · home → cart 宽松跟随：followedBy + within，对照 C2 contiguity。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e10-cep \
 *          -Dexec.mainClass=com.flywhl.flinklab.e10.C6HomeThenCartJob
 */
public final class C6HomeThenCartJob {
    private C6HomeThenCartJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var keyed = Labs.events(env, "cep6", 25, 4, 8, 800)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)))
                .keyBy(e -> e.userId);

        Pattern<Event, ?> pattern = Pattern.<Event>begin("home")
                .where(SimpleCondition.of(e -> "home".equals(e.page)))
                .followedBy("cart")
                .where(SimpleCondition.of(e -> "cart".equals(e.page)))
                .within(Duration.ofSeconds(15));

        CEP.pattern(keyed, pattern)
           .process(new PatternProcessFunction<Event, String>() {
               @Override
               public void processMatch(Map<String, List<Event>> match, Context ctx,
                                        Collector<String> out) {
                   Event h = match.get("home").get(0);
                   Event c = match.get("cart").get(0);
                   out.collect("FUNNEL user=%s homeTs=%d cartTs=%d"
                           .formatted(h.userId, h.ts, c.ts));
               }
           })
           .uid("e10-c6-home-cart")
           .print();

        env.execute("e10-c6-home-then-cart");
    }
}
