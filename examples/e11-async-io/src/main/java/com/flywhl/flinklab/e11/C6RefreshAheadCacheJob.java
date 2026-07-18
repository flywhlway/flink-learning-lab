package com.flywhl.flinklab.e11;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e11-C6 · 预刷新缓存：命中但临近过期时标记 REFRESH，逼近 TTL 边界再查。
 *
 * <p>不引入 Redis；用 ValueState 存 (profile, expireAt) 演示 refresh-ahead。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e11-async-io \
 *          -Dexec.mainClass=com.flywhl.flinklab.e11.C6RefreshAheadCacheJob
 */
public final class C6RefreshAheadCacheJob {
    private C6RefreshAheadCacheJob() {
    }

    private static final long TTL_MS = 3_000;
    private static final long REFRESH_AHEAD_MS = 1_000;

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "refresh", 12, 3, 10, 150)
            .keyBy(e -> e.userId)
            .process(new RefreshAhead())
            .uid("e11-c6-refresh")
            .print();
        env.execute("e11-c6-refresh-ahead-cache");
    }

    public static final class RefreshAhead extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<String> profile;
        private transient ValueState<Long> expireAt;

        @Override
        public void open(OpenContext ctx) {
            profile = getRuntimeContext().getState(new ValueStateDescriptor<>("prof", String.class));
            expireAt = getRuntimeContext().getState(new ValueStateDescriptor<>("exp", Long.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            long now = ctx.timerService().currentProcessingTime();
            Long exp = expireAt.value();
            String p = profile.value();
            if (p == null || exp == null || now >= exp) {
                p = "profile-" + e.userId + "@" + e.page;
                profile.update(p);
                expireAt.update(now + TTL_MS);
                out.collect("LOAD   user=%s %s".formatted(e.userId, p));
                return;
            }
            if (exp - now <= REFRESH_AHEAD_MS) {
                out.collect("REFRESH user=%s remainMs=%d keep=%s"
                        .formatted(e.userId, exp - now, p));
                // 异步刷新简化为同步更新过期时间
                expireAt.update(now + TTL_MS);
            } else {
                out.collect("HIT    user=%s %s".formatted(e.userId, p));
            }
        }
    }
}
