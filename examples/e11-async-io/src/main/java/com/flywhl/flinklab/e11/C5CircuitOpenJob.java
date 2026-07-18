package com.flywhl.flinklab.e11;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * e11-C5 · 熔断风格：Keyed 计数连续失败，打开后短路跳过 Async 外呼。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e11-async-io \
 *          -Dexec.mainClass=com.flywhl.flinklab.e11.C5CircuitOpenJob
 */
public final class C5CircuitOpenJob {
    private C5CircuitOpenJob() {
    }

    /** 门控后的事件：open=true 表示熔断打开。 */
    public static final class Gate {
        public Event event;
        public boolean open;

        public Gate() {
        }

        public Gate(Event event, boolean open) {
            this.event = event;
            this.open = open;
        }
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Gate> gated = Labs.events(env, "circuit", 15, 3, 8, 200)
                .keyBy(e -> e.userId)
                .process(new FailureGate())
                .uid("e11-c5-gate");

        AsyncDataStream.unorderedWait(gated, new ConditionalDim(), 1, TimeUnit.SECONDS, 50)
                .uid("e11-c5-async")
                .print();

        env.execute("e11-c5-circuit-open");
    }

    public static final class FailureGate extends KeyedProcessFunction<String, Event, Gate> {
        private transient ValueState<Integer> fails;

        @Override
        public void open(OpenContext ctx) {
            fails = getRuntimeContext().getState(new ValueStateDescriptor<>("fails", Integer.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<Gate> out) throws Exception {
            int f = fails.value() == null ? 0 : fails.value();
            if (((long) e.amount) % 5 == 0) {
                f++;
                fails.update(f);
            } else if (f > 0) {
                f = Math.max(0, f - 1);
                fails.update(f);
            }
            out.collect(new Gate(e, f >= 3));
        }
    }

    public static final class ConditionalDim extends RichAsyncFunction<Gate, String> {
        private transient FakeDimClient client;
        private transient ExecutorService pool;

        @Override
        public void open(OpenContext ctx) {
            client = new FakeDimClient(30, 0.1);
            pool = Executors.newFixedThreadPool(4);
        }

        @Override
        public void asyncInvoke(Gate g, ResultFuture<String> rf) {
            if (g.open) {
                rf.complete(Collections.singleton(
                        "SHORT-CIRCUIT user=%s".formatted(g.event.userId)));
                return;
            }
            client.lookup(g.event.userId, pool).whenComplete((p, err) -> {
                if (err != null) {
                    rf.completeExceptionally(err);
                } else {
                    rf.complete(Collections.singleton(
                            "CALL user=%s %s".formatted(g.event.userId, p)));
                }
            });
        }

        @Override
        public void timeout(Gate g, ResultFuture<String> rf) {
            rf.complete(Collections.singleton(
                    "TIMEOUT user=%s".formatted(g.event.userId)));
        }

        @Override
        public void close() {
            if (pool != null) {
                pool.shutdown();
            }
        }
    }
}
