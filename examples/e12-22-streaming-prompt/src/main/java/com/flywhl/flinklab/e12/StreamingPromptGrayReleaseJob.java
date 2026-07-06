package com.flywhl.flinklab.e12;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * ai/第22章 Demo · Prompt 版本化与确定性灰度分流(e03-C7 Broadcast 模式复用)。
 *
 * <p>模拟请求流(每条携带一个稳定的 entityId)与 Prompt 版本配置流(stable/canary
 * 两版本 + 灰度比例)。核心验证点:同一个 entityId 在整个运行期间始终被分配到
 * 同一个 Prompt 版本(确定性哈希分流),不会出现"这次 stable、下次 canary"的跳变。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-22-streaming-prompt \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingPromptGrayReleaseJob
 * 验证:筛选输出中同一 entity= 的行,version 字段应恒定不变。
 */
public final class StreamingPromptGrayReleaseJob {
    private StreamingPromptGrayReleaseJob() {
    }

    public static class PromptVersion {
        public String slot;          // "stable" 或 "canary"
        public String version;
        public double trafficRatio;  // 仅 canary 有意义

        public PromptVersion() {
        }

        public PromptVersion(String slot, String version, double ratio) {
            this.slot = slot; this.version = version; this.trafficRatio = ratio;
        }
    }

    public static class Request {
        public String entityId;

        public Request(String entityId) {
            this.entityId = entityId;
        }
    }

    private static final MapStateDescriptor<String, PromptVersion> PROMPTS_DESC =
            new MapStateDescriptor<>("prompt-versions", String.class, PromptVersion.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        GeneratorFunction<Long, Request> reqGen = idx ->
                new Request("entity-" + (idx % 15));   // 15 个稳定实体循环出现
        DataStream<Request> requests = env.fromSource(
                new DataGeneratorSource<>(reqGen, Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(10), TypeInformation.of(Request.class)),
                WatermarkStrategy.noWatermarks(), "request-source");

        GeneratorFunction<Long, PromptVersion> promptGen = idx ->
                idx == 0
                    ? new PromptVersion("stable", "v1.0", 0)
                    : new PromptVersion("canary", "v2.0-beta", 0.3);   // 30% 灰度流量
        BroadcastStream<PromptVersion> prompts = env.fromSource(
                        new DataGeneratorSource<>(promptGen, 2L,
                                RateLimiterStrategy.perSecond(0.5), TypeInformation.of(PromptVersion.class)),
                        WatermarkStrategy.noWatermarks(), "prompt-source")
                .broadcast(PROMPTS_DESC);

        requests.keyBy(r -> r.entityId)
                .connect(prompts)
                .process(new GrayReleaseRouter())
                .uid("e12-22-gray-release")
                .print();

        env.execute("e12-22-streaming-prompt-gray-release");
    }

    /** 确定性哈希分流:同一 entityId 始终路由到同一 Prompt 版本。 */
    public static final class GrayReleaseRouter
            extends KeyedBroadcastProcessFunction<String, Request, PromptVersion, String> {

        @Override
        public void processElement(Request req, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            ReadOnlyBroadcastState<String, PromptVersion> prompts = ctx.getBroadcastState(PROMPTS_DESC);
            PromptVersion stable = prompts.get("stable");
            PromptVersion canary = prompts.get("canary");

            boolean useCanary = canary != null
                    && Math.floorMod(req.entityId.hashCode(), 100) < canary.trafficRatio * 100;
            PromptVersion chosen = useCanary ? canary : stable;

            out.collect("entity=%s → version=%s (%s)".formatted(
                    req.entityId, chosen == null ? "PENDING(规则未下发)" : chosen.version,
                    useCanary ? "canary" : "stable"));
        }

        @Override
        public void processBroadcastElement(PromptVersion pv, Context ctx, Collector<String> out)
                throws Exception {
            ctx.getBroadcastState(PROMPTS_DESC).put(pv.slot, pv);
            out.collect("PROMPT-UPDATED slot=%s version=%s ratio=%.0f%%"
                    .formatted(pv.slot, pv.version, pv.trafficRatio * 100));
        }
    }
}
