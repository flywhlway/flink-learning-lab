package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * e03-C7 · Broadcast State:告警阈值规则的动态下发(改配置不重启)。
 *
 * <p>主流(订单)keyBy 用户;规则流(page→金额阈值)每 5s 变一次并 broadcast 到
 * 所有并行实例。KeyedBroadcastProcessFunction 两个入口:
 * processBroadcastElement 写规则(所有 subtask 各写一份、内容必须一致),
 * processElement 只读规则并判断告警。
 *
 * <p>这是生产四大金刚场景之一(动态规则/维表广播/AB 开关/黑白名单)——
 * 也是 CEP 动态化(e10)与 AI Prompt 灰度(ai/22)的底层机制。
 */
public final class C7BroadcastRuleJob {
    private C7BroadcastRuleJob() {
    }

    /** 规则:某 page 的告警阈值。 */
    public static class Rule {
        public String page;
        public double threshold;

        public Rule() {
        }

        public Rule(String page, double threshold) {
            this.page = page;
            this.threshold = threshold;
        }
    }

    private static final MapStateDescriptor<String, Rule> RULES_DESC =
            new MapStateDescriptor<>("rules", String.class, Rule.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Event> orders = Labs.events(env, "orders", 30, 6, 6, 500);

        GeneratorFunction<Long, Rule> ruleGen = idx -> {
            String[] pages = {"/pay", "/cart", "/item"};
            var rnd = ThreadLocalRandom.current();
            return new Rule(pages[(int) (idx % pages.length)], rnd.nextDouble(300, 480));
        };
        BroadcastStream<Rule> rules = env
                .fromSource(new DataGeneratorSource<>(ruleGen, Long.MAX_VALUE,
                                RateLimiterStrategy.perSecond(0.2),   // 每 5s 一条规则更新
                                TypeInformation.of(Rule.class)),
                        WatermarkStrategy.noWatermarks(), "rule-source")
                .broadcast(RULES_DESC);

        orders.keyBy(e -> e.userId)
              .connect(rules)
              .process(new KeyedBroadcastProcessFunction<String, Event, Rule, String>() {
                  @Override
                  public void processElement(Event e, ReadOnlyContext ctx, Collector<String> out)
                          throws Exception {
                      ReadOnlyBroadcastState<String, Rule> st =
                              ctx.getBroadcastState(RULES_DESC);
                      Rule r = st.get(e.page);
                      double th = r == null ? 490 : r.threshold; // 无规则时的兜底阈值
                      if (e.amount > th) {
                          out.collect("ALERT user=%s page=%s amount=%.1f > threshold=%.1f"
                                  .formatted(e.userId, e.page, e.amount, th));
                      }
                  }

                  @Override
                  public void processBroadcastElement(Rule r, Context ctx, Collector<String> out)
                          throws Exception {
                      ctx.getBroadcastState(RULES_DESC).put(r.page, r);
                      out.collect("RULE-UPDATED page=%s threshold=%.1f".formatted(r.page, r.threshold));
                  }
              })
              .uid("e03-c7-broadcast")
              .print();

        env.execute("e03-c7-broadcast-rules");
    }
}
