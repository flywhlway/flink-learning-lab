package com.flywhl.flinklab.e07;

import com.flywhl.flinklab.e07.customsource.RangeSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * e07-C5 · 自定义 FLIP-27 Source 解剖:四个部件各司其职。
 *
 * <p>RangeSource(装配)+ RangeEnumerator(JM 侧分片台账,pull 模型分发)+
 * RangeSourceReader(TM 侧真正读取,pollNext 主循环)+ 两个序列化器
 * (分片与枚举器状态如何随 checkpoint 落盘)。这是理解 Kafka/CDC connector
 * 内部实现的钥匙 —— 它们都是这四个部件的复杂版本。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e07-connectors \
 *          -Dexec.mainClass=com.flywhl.flinklab.e07.C5CustomFlip27SourceJob
 * 预期:0~999 共 1000 个数字被 4 个并行 reader 分段读出后求和打印,总和 = 499500。
 */
public final class C5CustomFlip27SourceJob {
    private C5CustomFlip27SourceJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.fromSource(new RangeSource(1000, 4), WatermarkStrategy.noWatermarks(), "range-source")
           .returns(Long.class)
           .keyBy(x -> 0L)
           .sum(0)
           .uid("e07-c5-sum")
           .print();

        env.execute("e07-c5-custom-flip27-source");
    }
}
