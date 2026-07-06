package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ai/第02章 Demo · 事件契约与消费者组隔离(本地模拟版,不依赖真实 Kafka)。
 *
 * <p>演示两件事:
 * ① 命令类 vs 事实类事件的产出方式差异(见 CommandEvent/FactEvent 两个契约类);
 * ② 同一份"事实"事件被两个独立逻辑(风控/监控大屏)各自完整消费一遍,
 *    互不干扰——这是 Kafka 消费者组"广播语义"的教学模拟;真实集群环境下
 *    换成两个 group.id 不同的 KafkaSource 即是 ai/02 第4节的生产版本。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e12-02-event-bus \
 *          -Dexec.mainClass=com.flywhl.flinklab.e12.EventBusTopicDesignJob
 */
public final class EventBusTopicDesignJob {
    private EventBusTopicDesignJob() {
    }

    /** 事实类事件契约:允许被多方订阅,不隐含"谁来处理"。 */
    public static class RiskSignalFactV1 {
        public final String schemaVersion = "v1";
        public String vin;       // 分区键
        public String signalType;
        public double value;
        public long eventTimeMs;

        public RiskSignalFactV1(String vin, String signalType, double value, long ts) {
            this.vin = vin; this.signalType = signalType; this.value = value; this.eventTimeMs = ts;
        }

        @Override
        public String toString() {
            return "RiskSignalFact{v=%s,vin=%s,type=%s,val=%.1f}"
                    .formatted(schemaVersion, vin, signalType, value);
        }
    }

    /** 命令类事件契约:语义上只应被处理一次,不广播。 */
    public static class DispatchAlertCommandV1 {
        public final String schemaVersion = "v1";
        public String vin;
        public String message;

        public DispatchAlertCommandV1(String vin, String message) {
            this.vin = vin; this.message = message;
        }

        @Override
        public String toString() {
            return "DispatchAlertCommand{v=%s,vin=%s,msg=%s}".formatted(schemaVersion, vin, message);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> raw = Labs.events(env, "can-bus", 20, 5, 8, 500);

        // 富化成"事实类"契约事件(ods.raw → dwd.enriched 的分层语义,ai/02 第2节)
        DataStream<RiskSignalFactV1> facts = raw.map(e ->
                new RiskSignalFactV1(e.userId, e.page, e.amount, e.ts))
                .uid("e12-02-enrich");

        // 消费者组 1:风控逻辑——完整订阅一遍事实流
        facts.filter(f -> f.value > 400)
             .map(f -> "[风控 Agent] " + f + " → 触发风险评估")
             .uid("e12-02-risk-consumer")
             .print();

        // 消费者组 2:监控大屏——完整订阅同一份事实流,独立于风控逻辑
        facts.map(f -> "[监控大屏]   " + f + " → 更新看板")
             .uid("e12-02-dashboard-consumer")
             .print();

        // 命令类事件示例:只在真正需要下发告警时产生,且语义上不应被"广播消费"
        facts.filter(f -> f.value > 480)
             .map(f -> new DispatchAlertCommandV1(f.vin, "高风险信号 " + f.signalType))
             .map(cmd -> "[单一执行者] 收到命令 → " + cmd)
             .uid("e12-02-command-consumer")
             .print();

        env.execute("e12-02-event-bus-topic-design");
    }
}
