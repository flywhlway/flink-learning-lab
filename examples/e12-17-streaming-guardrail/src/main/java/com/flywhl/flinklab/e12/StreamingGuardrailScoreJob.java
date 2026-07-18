package com.flywhl.flinklab.e12;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** e12-17 第二 Job · 基于 amount 的简易风险分护栏（无 Broadcast，对照主 Job）。 */
public final class StreamingGuardrailScoreJob {
    private StreamingGuardrailScoreJob() {}

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "guard-score", 20, 4, 5, 200)
            .process(new ScoreGuard())
            .uid("e12-17-score-guard")
            .print();
        env.execute("e12-17-streaming-guardrail-score");
    }

    public static final class ScoreGuard extends ProcessFunction<Event, String> {
        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) {
            double score = e.amount / 500.0;
            if (score > 0.85) {
                out.collect("BLOCK score=%.2f user=%s".formatted(score, e.userId));
            } else {
                out.collect("PASS  score=%.2f user=%s".formatted(score, e.userId));
            }
        }
    }
}
