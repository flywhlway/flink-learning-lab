package com.flywhl.flinklab.e04;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * e04-C1 · 对齐 vs 非对齐 checkpoint:反压现场的对照实验(本地 WebUI 观察)。
 *
 * <p>拓扑:source → rebalance → 慢 map(每条 sleep 5ms,人为反压)→ print。
 * rebalance 边保证存在跨网络 channel(barrier 对齐才有意义)。
 *
 * <p>两次运行对比:
 * <pre>
 *   # 第一轮:对齐(默认)
 *   mvn -q -Plocal compile exec:java -pl e04-checkpoint \
 *       -Dexec.mainClass=com.flywhl.flinklab.e04.C1AlignedVsUnalignedJob
 *   # 第二轮:非对齐
 *   ... -Dexec.args="--unaligned"
 * </pre>
 * 打开 http://localhost:8082 → Checkpoints:
 * 对齐模式下 End to End Duration 随反压显著拉长(barrier 排队),
 * 非对齐模式 barrier 越流而过,Duration 回落但 checkpoint 体积变大(in-flight 数据入湖)。
 * 这就是"非对齐 = 用存储换时效"的直观证据。
 */
public final class C1AlignedVsUnalignedJob {
    private C1AlignedVsUnalignedJob() {
    }

    public static void main(String[] args) throws Exception {
        boolean unaligned = args.length > 0 && "--unaligned".equals(args[0]);

        Map<String, String> cfg = new HashMap<>();
        cfg.put("rest.port", "8082");
        cfg.put("execution.checkpointing.dir", "file:///tmp/flink-lab/e04-c1");
        cfg.put("execution.checkpointing.unaligned.enabled", String.valueOf(unaligned));
        Configuration conf = Configuration.fromMap(cfg);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        env.enableCheckpointing(10_000);

        System.out.println(">>> mode = " + (unaligned ? "UNALIGNED" : "ALIGNED")
                + " · WebUI http://localhost:8082");

        Labs.events(env, "load", 800, 20, 10, 500)
            .rebalance()                       // 制造跨 channel 边
            .map(e -> {
                Thread.sleep(5);               // 人为慢算子 → 反压
                return e.page;
            })
            .name("slow-map").uid("e04-c1-slow")
            .print();

        env.execute("e04-c1-" + (unaligned ? "unaligned" : "aligned"));
    }
}
