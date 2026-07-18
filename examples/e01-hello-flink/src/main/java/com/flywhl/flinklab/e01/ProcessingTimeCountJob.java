package com.flywhl.flinklab.e01;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * e01-J4 · 处理时间滚动窗口计数：与 J1 事件时间对照，说明「墙钟窗口」适用场景。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e01-hello-flink \
 *          -Dexec.mainClass=com.flywhl.flinklab.e01.ProcessingTimeCountJob
 */
public final class ProcessingTimeCountJob {
    private ProcessingTimeCountJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "pt-count", 20, 4, 5, 200)
            .keyBy(e -> e.page)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(3)))
            .process(new CountPages())
            .uid("e01-j4-pt-window")
            .print();
        env.execute("e01-processing-time-count");
    }

    public static final class CountPages
            extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String page, Context ctx, Iterable<Event> elements, Collector<String> out) {
            long n = 0;
            for (Event ignored : elements) {
                n++;
            }
            out.collect("PT-WIN page=%s count=%d windowEnd=%d"
                    .formatted(page, n, ctx.window().getEnd()));
        }
    }
}
