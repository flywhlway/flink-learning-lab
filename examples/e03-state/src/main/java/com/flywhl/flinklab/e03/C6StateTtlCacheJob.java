package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * e03-C6 · State TTL:带过期的"画像缓存"。
 *
 * <p>生成器是突发式(用户活跃一阵→沉默一轮):TTL=8s + 用户沉默期 >8s
 * → 用户回归时状态已过期,打印 MISS(rebuild);活跃期内命中打印 HIT。
 *
 * <p>TTL 语义三件事必须一起讲清:① 计时基于**处理时间**;② 过期数据默认
 * 惰性清理(读到才删)+ 后台清理,不是到点即删;③ NeverReturnExpired 保证
 * 过期即不可见 —— 但状态占用的释放仍是异步的,容量规划别按"到点归零"算。
 */
public final class C6StateTtlCacheJob {
    private C6StateTtlCacheJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "profile", 10, 6, 8, 200)
            .keyBy(e -> e.userId)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private transient ValueState<String> profile;

                @Override
                public void open(OpenContext ctx) {
                    StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofSeconds(8))
                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .build();
                    ValueStateDescriptor<String> desc =
                            new ValueStateDescriptor<>("profile-cache", String.class);
                    desc.enableTimeToLive(ttl);
                    profile = getRuntimeContext().getState(desc);
                }

                @Override
                public void processElement(Event e, Context ctx, Collector<String> out)
                        throws Exception {
                    String p = profile.value();
                    if (p == null) {
                        p = "profile-of-" + e.userId;  // 模拟一次昂贵的画像构建/外部查询
                        profile.update(p);
                        out.collect("MISS(rebuild) user=" + e.userId);
                    } else {
                        out.collect("HIT           user=" + e.userId);
                    }
                }
            })
            .uid("e03-c6-ttl")
            .print();

        env.execute("e03-c6-state-ttl-cache");
    }
}
