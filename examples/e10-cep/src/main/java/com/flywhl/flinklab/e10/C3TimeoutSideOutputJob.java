package com.flywhl.flinklab.e10;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * e10-C3 · 超时旁路:加购(/cart)后 10s 内未支付 → 挽单侧输出。
 *
 * <p>CEP 最有商业价值的能力恰恰是"没发生":实现 TimedOutPartialMatchHandler,
 * 超时的**半成品匹配**(只有 cart 没有 pay)从 side output 流出 → 接催付/优惠券。
 * 主输出仍是完成转化的用户。这是"超时营销/风控静默检测"的骨架,
 * 案例三的 DTC 上报超时告警同构。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e10-cep \
 *          -Dexec.mainClass=com.flywhl.flinklab.e10.C3TimeoutSideOutputJob
 */
public final class C3TimeoutSideOutputJob {
    private C3TimeoutSideOutputJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final OutputTag<String> abandoned = new OutputTag<>("abandoned") {
        };

        var keyed = Labs.events(env, "cart", 30, 5, 8, 1_000)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)))
                .keyBy(e -> e.userId);

        Pattern<Event, ?> pattern = Pattern.<Event>begin("cart")
                .where(SimpleCondition.of(e -> "/cart".equals(e.page)))
                .followedBy("pay")
                .where(SimpleCondition.of(e -> "/pay".equals(e.page)))
                .within(Duration.ofSeconds(10));

        SingleOutputStreamOperator<String> converted =
                CEP.pattern(keyed, pattern).process(new CartHandler(abandoned));

        converted.print();                              // 完成转化
        converted.getSideOutput(abandoned).print();     // 超时挽单

        env.execute("e10-c3-timeout-side-output");
    }

    /** 主匹配 + 超时半成品双处理。 */
    public static final class CartHandler
            extends PatternProcessFunction<Event, String>
            implements TimedOutPartialMatchHandler<Event> {

        private final OutputTag<String> abandoned;

        public CartHandler(OutputTag<String> abandoned) {
            this.abandoned = abandoned;
        }

        @Override
        public void processMatch(Map<String, List<Event>> m, Context ctx, Collector<String> out) {
            out.collect("CONVERTED user=" + m.get("pay").get(0).userId);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<Event>> m, Context ctx) {
            ctx.output(abandoned, "ABANDONED-CART user=" + m.get("cart").get(0).userId
                    + " → 触发挽单(优惠券/推送)");
        }
    }
}
