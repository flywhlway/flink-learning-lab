package com.flywhl.flinklab.e12;

import org.apache.flink.agents.api.AgentsExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ai/第07章 Demo · 装配:把 SimpleThresholdAgent 接入 Flink 流拓扑。
 *
 * <p>本地(IDE)运行前置:
 * ① IDE 开启 "add dependencies with provided scope to classpath";
 * ② JDK 21 运行需 JVM 参数 --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
 *    (Agents 的 executeAsync 基于 Continuation,虽然本例未用到,框架初始化仍要求);
 * ③ 集群运行:该参数追加到 $FLINK_HOME/conf/config.yaml 的 env.java.opts.all。
 *
 * <p>运行(本模块独立构建,不走父 pom):
 *   cd examples/e12-07-agent-quickstart
 *   mvn -q compile exec:java -Dexec.mainClass=com.flywhl.flinklab.e12.AgentQuickstartJob \
 *       -Dexec.jvmArgs="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
 * 预期:约 20% 的信号超阈值,输出 ALERT 行。
 */
public final class AgentQuickstartJob {
    private AgentQuickstartJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Agents 执行环境包裹 Flink 环境(官方 Quickstart 模式)
        AgentsExecutionEnvironment agentsEnv =
                AgentsExecutionEnvironment.getExecutionEnvironment(env);

        // 有界示例数据:50 条信号,约 1/5 超阈值
        List<VehicleSignal> signals = new ArrayList<>();
        var rnd = ThreadLocalRandom.current();
        for (int i = 0; i < 50; i++) {
            signals.add(new VehicleSignal(
                    "VIN-%03d".formatted(i % 8),
                    rnd.nextBoolean() ? "MOTOR_TEMP" : "BATTERY_VOLT",
                    rnd.nextDouble(0, 125),
                    100));
        }
        DataStream<VehicleSignal> input = env.fromData(signals);

        agentsEnv.fromDataStream(input)
                 .apply(SimpleThresholdAgent.class)
                 .toDataStream()
                 .print();

        agentsEnv.execute();
    }
}
