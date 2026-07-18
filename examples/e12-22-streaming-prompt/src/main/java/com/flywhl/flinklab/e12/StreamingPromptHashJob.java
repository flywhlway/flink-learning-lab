package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** e12-22 第二 Job · Prompt 指纹：page+模板版本哈希，便于灰度对照。 */
public final class StreamingPromptHashJob {
    private StreamingPromptHashJob() {}
    private static final String TEMPLATE_VER = "v2";

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "prompt-hash", 15, 4, 5, 200)
            .process(new PromptHash())
            .uid("e12-22-prompt-hash")
            .print();
        env.execute("e12-22-streaming-prompt-hash");
    }

    public static final class PromptHash extends ProcessFunction<Event, String> {
        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) {
            String raw = TEMPLATE_VER + "|" + e.page + "|" + e.userId;
            int fp = raw.hashCode();
            out.collect("PROMPT ver=%s user=%s fp=%d".formatted(TEMPLATE_VER, e.userId, fp));
        }
    }
}
