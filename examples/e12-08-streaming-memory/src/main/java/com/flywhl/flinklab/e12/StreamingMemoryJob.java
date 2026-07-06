package com.flywhl.flinklab.e12;

import org.apache.flink.agents.api.AgentsExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ai/第08章 Demo · 装配:短期记忆 Agent 接入流拓扑。
 *
 * <p>注意 keyBy 语义:短期记忆按 key 隔离(每个 vin 独立一份记忆),
 * 输入流需按 vin 分区,与 e03 Keyed State 的分区语义完全一致。
 *
 * <p>运行(独立构建 + JVM 参数,同 e12-07):
 *   cd examples/e12-08-streaming-memory
 *   mvn -q compile exec:java -Dexec.mainClass=com.flywhl.flinklab.e12.StreamingMemoryJob \
 *       -Dexec.jvmArgs="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
 * 预期:同一 vin 的第 2 条起输出 RISING/FALLING/FLAT(证明记忆跨运行保留),
 * 每个 vin 的第 1 条输出 FIRST。
 */
public final class StreamingMemoryJob {
    private StreamingMemoryJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        AgentsExecutionEnvironment agentsEnv =
                AgentsExecutionEnvironment.getExecutionEnvironment(env);

        // 3 个 vin,每个 10 条信号,值随机波动 → 观察 trend 输出
        List<VehicleSignal> signals = new ArrayList<>();
        var rnd = ThreadLocalRandom.current();
        for (int round = 0; round < 10; round++) {
            for (int v = 0; v < 3; v++) {
                signals.add(new VehicleSignal(
                        "VIN-%03d".formatted(v), "MOTOR_TEMP",
                        60 + rnd.nextDouble(-20, 20), 100));
            }
        }
        DataStream<VehicleSignal> input = env.fromData(signals)
                .keyBy(s -> s.vin);   // 记忆按 key 隔离,必须先 keyBy

        agentsEnv.fromDataStream(input)
                 .apply(MemoryAgent.class)
                 .toDataStream()
                 .print();

        agentsEnv.execute();
    }
}
