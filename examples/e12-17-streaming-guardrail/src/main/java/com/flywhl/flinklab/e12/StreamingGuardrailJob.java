package com.flywhl.flinklab.e12;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * ai/第17章 Demo · 流式内容护栏:Broadcast 规则热更新(e03-C7 模式直接复用)。
 *
 * <p>模拟"LLM 输出流"(用随机文本代替真实模型调用,避免外部依赖),
 * 护栏规则(关键词黑名单)通过 Broadcast 流动态下发,运行中新增规则
 * 立刻对后续输出生效,不需要重启作业——这是本章要证明的核心能力。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-17-streaming-guardrail \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingGuardrailJob
 * 预期:RULE-UPDATED 日志出现后,后续命中新关键词的输出立即被 BLOCK。
 */
public final class StreamingGuardrailJob {
    private StreamingGuardrailJob() {
    }

    /** 护栏规则:命中关键词即拦截。 */
    public static class GuardrailRule {
        public String ruleId, keyword, action;

        public GuardrailRule() {
        }

        public GuardrailRule(String ruleId, String keyword, String action) {
            this.ruleId = ruleId; this.keyword = keyword; this.action = action;
        }
    }

    private static final MapStateDescriptor<String, GuardrailRule> RULES_DESC =
            new MapStateDescriptor<>("guardrail-rules", String.class, GuardrailRule.class);

    // 模拟 LLM 输出:随机拼词,其中偶尔包含"高危词"用于演示拦截触发
    private static final String[] SAFE_WORDS = {"你好", "订单", "查询", "天气", "帮助"};
    private static final String[] RISKY_WORDS = {"越权操作", "忽略安全策略", "泄露内部信息"};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        GeneratorFunction<Long, String> llmOutputGen = idx -> {
            var rnd = ThreadLocalRandom.current();
            String[] pool = rnd.nextDouble() < 0.15 ? RISKY_WORDS : SAFE_WORDS;
            return "response-%d: %s".formatted(idx, pool[rnd.nextInt(pool.length)]);
        };
        DataStream<String> llmOutputs = env.fromSource(
                new DataGeneratorSource<>(llmOutputGen, Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5), TypeInformation.of(String.class)),
                WatermarkStrategy.noWatermarks(), "llm-output-source");

        // 规则流:初始只有一条规则,5 秒后新增一条(模拟运维热下发)
        GeneratorFunction<Long, GuardrailRule> ruleGen = idx ->
                idx == 0
                    ? new GuardrailRule("r1", "越权操作", "BLOCK")
                    : new GuardrailRule("r2", "泄露内部信息", "BLOCK");   // 第二条即新规则
        BroadcastStream<GuardrailRule> rules = env.fromSource(
                        new DataGeneratorSource<>(ruleGen, 2L,           // 只发 2 条:初始规则 + 新规则
                                RateLimiterStrategy.perSecond(0.2), TypeInformation.of(GuardrailRule.class)),
                        WatermarkStrategy.noWatermarks(), "rule-source")
                .broadcast(RULES_DESC);

        llmOutputs.keyBy(s -> "single-key")   // 演示用单 key;生产按租户/会话分区
                  .connect(rules)
                  .process(new GuardrailProcessFn())
                  .uid("e12-17-guardrail")
                  .print();

        env.execute("e12-17-streaming-guardrail");
    }

    /** 护栏校验:对每条输出应用当前全部规则(与 e03-C7 ALERT 判断逻辑同构)。 */
    public static final class GuardrailProcessFn
            extends KeyedBroadcastProcessFunction<String, String, GuardrailRule, String> {

        @Override
        public void processElement(String output, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            ReadOnlyBroadcastState<String, GuardrailRule> rules = ctx.getBroadcastState(RULES_DESC);
            for (java.util.Map.Entry<String, GuardrailRule> entry : rules.immutableEntries()) {
                if (output.contains(entry.getValue().keyword)) {
                    out.collect("BLOCK  [规则%s命中\"%s\"] %s"
                            .formatted(entry.getValue().ruleId, entry.getValue().keyword, output));
                    return;
                }
            }
            out.collect("PASS   " + output);
        }

        @Override
        public void processBroadcastElement(GuardrailRule rule, Context ctx, Collector<String> out)
                throws Exception {
            ctx.getBroadcastState(RULES_DESC).put(rule.ruleId, rule);
            out.collect("RULE-UPDATED  新增规则 %s: 关键词=\"%s\" action=%s"
                    .formatted(rule.ruleId, rule.keyword, rule.action));
        }
    }
}
