package com.flywhl.flinklab.template;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * DataStream 作业标准骨架(交付三件套字段见文末注释)。
 *
 * <p>本模板固化的纪律,均可在 best-practice/ 找到对应条款,不含"模板私货":
 * ① 参数从 JobConfig 统一解析(禁散落硬编码);
 * ② checkpoint 间隔/超时/后端显式配置,不用框架默认值蒙混过关(军规 1/2);
 * ③ 每个有状态/有意义的算子必须设置 uid(升级契约,docs/04-02);
 * ④ 外部系统访问一律走 Async I/O 骨架(占位见 buildPipeline 注释,军规 4);
 * ⑤ main 方法只做装配,业务逻辑下沉到独立的 Function 类,便于单测。
 *
 * <p>使用方式:复制本模块为新项目起点,重命名包名,在 buildPipeline 中
 * 替换 source/transform/sink 为业务逻辑,JobConfig 按需增删参数。
 */
public final class JobTemplate {
    private JobTemplate() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);

        Configuration conf = Configuration.fromMap(Map.of(
                "state.backend.type", cfg.stateBackendType,
                "execution.checkpointing.incremental", "true"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(cfg.checkpointTimeoutMs);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                org.apache.flink.streaming.api.environment.ExternalizedCheckpointRetention
                        .RETAIN_ON_CANCELLATION);

        buildPipeline(env, cfg);

        env.execute(cfg.jobName);
    }

    /**
     * 装配点:替换为业务 source → transform → sink。
     * 外呼富化请参照 e11-async-io 的 Async I/O 骨架(orderedWait/unorderedWaitWithRetry
     * + timeout() 降级),不要在 map/process 里同步调用外部系统。
     */
    private static void buildPipeline(StreamExecutionEnvironment env, JobConfig cfg) {
        // 替换点(项目方): 替换为真实 source(参照 e07 connectors 选型决策图)
        // 替换点(项目方): 替换为真实 transform(有状态算子记得 .uid(...) + 设计文档登记状态四问,docs/03)
        // 替换点(项目方): 替换为真实 sink(投递语义先问"下游允许重复吗",docs/04-04)
        throw new UnsupportedOperationException(
                "模板占位:请在 buildPipeline 中装配真实 source/transform/sink");
    }
}

/*
 * ══════════════ 上线交付三件套(评审时逐项确认) ══════════════
 * 1. 容错档案:checkpoint 间隔/超时/后端与增量开关/Sink 投递语义/依据
 * 2. 状态档案:每个有状态算子的类型/预估规模/TTL策略/演进契约(uid+状态名)
 * 3. 演练记录:至少完成一次 stop-with-savepoint 升级演练 + 一次故障恢复演练
 * (参照 e04 四个案例的实验设计;模板不代做,但结构必须能装下这三份档案)
 */
