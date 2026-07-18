package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
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

/**
 * ai/第19章 Demo · AI Gateway：Broadcast 路由表按租户/模型分流。
 *
 * <p>请求流按 userId 进入；路由规则(Broadcast)决定走 cheap 还是 premium 模型。
 * 规则热更新后立即生效——网关不必重启作业。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-19-ai-gateway-route \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.AiGatewayRouteJob
 */
public final class AiGatewayRouteJob {
    private AiGatewayRouteJob() {
    }

    public static class RouteRule {
        public String tenantPrefix;
        public String modelId;

        public RouteRule() {
        }

        public RouteRule(String tenantPrefix, String modelId) {
            this.tenantPrefix = tenantPrefix;
            this.modelId = modelId;
        }
    }

    private static final MapStateDescriptor<String, RouteRule> ROUTE_DESC =
            new MapStateDescriptor<>("routes", String.class, RouteRule.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> requests = Labs.events(env, "gw-req", 15, 6, 4, 200);

        GeneratorFunction<Long, RouteRule> ruleGen = idx ->
                idx == 0
                        ? new RouteRule("u0", "qwen-cheap")
                        : new RouteRule("u1", "qwen-premium");
        BroadcastStream<RouteRule> routes = env.fromSource(
                        new DataGeneratorSource<>(ruleGen, 2L,
                                RateLimiterStrategy.perSecond(0.3),
                                TypeInformation.of(RouteRule.class)),
                        WatermarkStrategy.noWatermarks(), "route-source")
                .broadcast(ROUTE_DESC);

        requests.keyBy(e -> e.userId)
                .connect(routes)
                .process(new GatewayRouter())
                .uid("e12-19-gateway")
                .print();

        env.execute("e12-19-ai-gateway-route");
    }

    public static final class GatewayRouter
            extends KeyedBroadcastProcessFunction<String, Event, RouteRule, String> {
        @Override
        public void processElement(Event req, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            ReadOnlyBroadcastState<String, RouteRule> st = ctx.getBroadcastState(ROUTE_DESC);
            String model = "default-local";
            for (var e : st.immutableEntries()) {
                RouteRule r = e.getValue();
                if (req.userId.startsWith(r.tenantPrefix)) {
                    model = r.modelId;
                    break;
                }
            }
            out.collect("ROUTE  user=%s model=%s page=%s".formatted(req.userId, model, req.page));
        }

        @Override
        public void processBroadcastElement(RouteRule rule, Context ctx, Collector<String> out)
                throws Exception {
            ctx.getBroadcastState(ROUTE_DESC).put(rule.tenantPrefix, rule);
            out.collect("RULE   tenant=%s -> model=%s".formatted(rule.tenantPrefix, rule.modelId));
        }
    }
}
