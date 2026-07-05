package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * e03-C5 · Operator State(CheckpointedFunction):攒批缓冲的容错化。
 *
 * <p>场景:向外部系统写入要攒批(每 10 条 flush 一次)。JVM 内存里的 buffer 一宕机就丢,
 * 用 Operator ListState 在 snapshotState 时落盘、initializeState 时恢复,
 * 攒批就获得了与 checkpoint 一致的可靠性。这是 SinkFunction 时代 BufferingSink 的
 * Flink 2.x 版本 —— 该模式如今活在 FlatMap/自定义 SinkV2 里。
 *
 * <p>要点:Operator State 与 key 无关、按 subtask 存;恢复时 getListState 是
 * even-split 重分发(每个 subtask 分到一部分),union 版本(getUnionListState)慎用。
 */
public final class C5OperatorStateBufferingJob {
    private C5OperatorStateBufferingJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5_000);

        Labs.events(env, "buffered", 15, 5, 6, 500)
            .flatMap(new BufferingFlatMap(10))
            .uid("e03-c5-buffer")
            .print();

        env.execute("e03-c5-operator-state-buffering");
    }

    /** 攒批 flatMap:volatile 内存 buffer + checkpoint 时同步进 Operator State。 */
    public static final class BufferingFlatMap
            implements FlatMapFunction<Event, String>, CheckpointedFunction {

        private final int threshold;
        private transient ListState<Event> checkpointed;
        private final List<Event> buffer = new ArrayList<>();

        public BufferingFlatMap(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void flatMap(Event e, Collector<String> out) {
            buffer.add(e);
            if (buffer.size() >= threshold) {
                out.collect("FLUSH batch(size=%d): first=%s last=%s".formatted(
                        buffer.size(), buffer.get(0).userId,
                        buffer.get(buffer.size() - 1).userId));
                buffer.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            checkpointed.update(buffer);   // 未 flush 的尾巴随 checkpoint 持久化
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            checkpointed = ctx.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("pending-buffer", Event.class));
            if (ctx.isRestored()) {
                for (Event e : checkpointed.get()) {
                    buffer.add(e);
                }
            }
        }
    }
}
