package com.flywhl.flinklab.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 实验工具箱:基于 FLIP-27 DataGeneratorSource 的无界数据源。
 *
 * <p>为什么不用自写 SourceFunction:Flink 2.0 已彻底移除 SourceFunction/SinkFunction,
 * 新 Source API(FLIP-27)是唯一正道;datagen 连接器就是官方给的"本地无界源"标准答案。
 */
public final class Labs {

    private Labs() {
    }

    /**
     * 突发式点击流:每个用户连续产出 {@code burst} 条后沉默一轮(供会话窗口/超时检测实验)。
     * 事件时间 = 当前墙钟 - [0, maxLagMs) 随机乱序。
     */
    public static DataStream<Event> events(StreamExecutionEnvironment env, String name,
                                           double eps, int users, int burst, long maxLagMs) {
        GeneratorFunction<Long, Event> gen = idx -> {
            var rnd = ThreadLocalRandom.current();
            String user = "u" + ((idx / burst) % users);
            String page = PAGES[rnd.nextInt(PAGES.length)];
            long ts = System.currentTimeMillis() - rnd.nextLong(maxLagMs + 1);
            return new Event(user, page, rnd.nextDouble(1, 500), ts);
        };
        return env.fromSource(
                new DataGeneratorSource<>(gen, Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(eps), TypeInformation.of(Event.class)),
                WatermarkStrategy.noWatermarks(), name);
    }

    /** 逻辑时钟事件流:事件时间从 baseTs 起每条前进 stepMs(供多流速度差/对齐实验)。 */
    public static DataStream<Event> pacedEvents(StreamExecutionEnvironment env, String name,
                                                double eps, long baseTs, long stepMs) {
        GeneratorFunction<Long, Event> gen = idx -> {
            var rnd = ThreadLocalRandom.current();
            return new Event("u" + rnd.nextInt(50), name,
                    rnd.nextDouble(1, 500), baseTs + idx * stepMs);
        };
        return env.fromSource(
                new DataGeneratorSource<>(gen, Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(eps), TypeInformation.of(Event.class)),
                WatermarkStrategy.noWatermarks(), name);
    }

    /** 标准乱序 watermark 策略(时间戳取业务字段,带空闲检测)。 */
    public static WatermarkStrategy<Event> boundedWm(Duration bound) {
        return WatermarkStrategy.<Event>forBoundedOutOfOrderness(bound)
                .withTimestampAssigner((e, ts) -> e.ts)
                .withIdleness(Duration.ofSeconds(30));
    }

    private static final String[] PAGES =
            {"/home", "/search", "/item", "/cart", "/pay", "/profile"};
}
