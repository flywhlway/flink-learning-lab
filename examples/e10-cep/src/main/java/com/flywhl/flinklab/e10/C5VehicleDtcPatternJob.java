package com.flywhl.flinklab.e10;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * e10-C5 · 车联网告警预演(案例三 p03 的模式雏形)。
 *
 * <p>把通用 Event 语义映射到车况:userId≈VIN,page≈信号类型,amount≈信号值。
 * 模式:「急加速信号(/search 且 值>450)之后 30s 内出现 故障信号(/pay 且 值>480)」
 * → 高优先级告警(疑似激烈驾驶诱发故障)。真实 p03 中该模式将换成
 * speed/DTC 专用 POJO + Broadcast 动态阈值(e03-C7 机制),骨架完全一致。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e10-cep \
 *          -Dexec.mainClass=com.flywhl.flinklab.e10.C5VehicleDtcPatternJob
 */
public final class C5VehicleDtcPatternJob {
    private C5VehicleDtcPatternJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var keyed = Labs.events(env, "can-bus", 60, 6, 15, 1_000)
                .assignTimestampsAndWatermarks(Labs.boundedWm(Duration.ofSeconds(1)))
                .keyBy(e -> e.userId);   // ≈ VIN

        Pattern<Event, ?> pattern = Pattern.<Event>begin("harsh")
                .where(SimpleCondition.of(e -> "/search".equals(e.page) && e.amount > 450))
                .followedBy("fault")
                .where(SimpleCondition.of(e -> "/pay".equals(e.page) && e.amount > 480))
                .within(Duration.ofSeconds(30));

        CEP.pattern(keyed, pattern)
           .process(new PatternProcessFunction<Event, String>() {
               @Override
               public void processMatch(Map<String, List<Event>> m, Context ctx,
                                        Collector<String> out) {
                   Event harsh = m.get("harsh").get(0);
                   Event fault = m.get("fault").get(0);
                   out.collect(("P1-ALERT vin=%s 激烈驾驶(%.0f)后 %.1fs 出现故障信号(%.0f)"
                           + " → 建议:下发远程诊断 + 通知车主")
                           .formatted(harsh.userId, harsh.amount,
                                   (fault.ts - harsh.ts) / 1000.0, fault.amount));
               }
           })
           .uid("e10-c5-dtc")
           .print();

        env.execute("e10-c5-vehicle-dtc-pattern");
    }
}
