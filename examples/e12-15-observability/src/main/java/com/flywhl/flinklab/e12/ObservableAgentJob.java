package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ai/第15章 Demo · Agent 可观测性三件套(Metrics + 结构化日志),不含 Preview API。
 *
 * <p>本 Demo 用一个"模拟 Agent 决策"算子演示:自定义 Counter/Histogram
 * 接入 Flink Metrics API(可直接被现有 Prometheus/Grafana 管道抓取,
 * docker-compose 已接通),以及结构化字段日志的输出约定。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-15-observability \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.ObservableAgentJob
 * 验证:本地跑通后,提交到集群(flink run)可在 :3000 Grafana / :8081 WebUI
 * 的算子指标里看到 alerts_triggered 计数与 decision_latency_ms 直方图。
 */
public final class ObservableAgentJob {
    private ObservableAgentJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "decisions", 30, 6, 8, 500)
            .keyBy(e -> e.userId)
            .process(new ObservableThresholdCheck())
            .uid("e12-15-observable-decision")
            .print();

        env.execute("e12-15-observability");
    }

    /** 模拟"Agent 决策"算子:每次处理都打点指标 + 结构化日志。 */
    public static final class ObservableThresholdCheck
            extends KeyedProcessFunction<String, Event, String> {

        private static final Logger LOG = LoggerFactory.getLogger(ObservableThresholdCheck.class);
        private static final double THRESHOLD = 450;

        private transient Counter alertCounter;
        private transient Histogram latencyHistogram;

        @Override
        public void open(OpenContext ctx) {
            alertCounter = getRuntimeContext().getMetricGroup()
                    .counter("alerts_triggered");
            latencyHistogram = getRuntimeContext().getMetricGroup()
                    .histogram("decision_latency_ms",
                            new DropwizardHistogramWrapper(
                                    new com.codahale.metrics.Histogram(
                                            new com.codahale.metrics.SlidingWindowReservoir(500))));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) {
            long start = System.nanoTime();
            boolean triggered = e.amount > THRESHOLD;
            long latencyMs = (System.nanoTime() - start) / 1_000_000;
            latencyHistogram.update(Math.max(latencyMs, 1));   // 演示用途,确保非零可观察

            // 结构化日志:字段化,便于 ClickHouse/ELK 按字段查询(ai/15 第4节)
            LOG.info("agent_decision event_user={} action=checkThreshold amount={} " +
                            "triggered={} latency_ms={}",
                    e.userId, e.amount, triggered, latencyMs);

            if (triggered) {
                alertCounter.inc();
                out.collect("ALERT user=%s amount=%.1f (metrics: alerts_triggered++, latency=%dms)"
                        .formatted(e.userId, e.amount, latencyMs));
            }
        }
    }
}
