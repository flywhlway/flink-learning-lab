package com.flywhl.flinklab.e04;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * e04-C6 · uid 契约演示：固定 uid + 状态名，说明 Savepoint 升级认领依据。
 *
 * <p>运行:mvn -q -Plocal compile exec:java -pl e04-checkpoint \
 *          -Dexec.mainClass=com.flywhl.flinklab.e04.C6UidContractDemoJob
 */
public final class C6UidContractDemoJob {
    private C6UidContractDemoJob() {
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Labs.events(env, "uid-demo", 20, 3, 6, 200)
            .keyBy(e -> e.userId)
            .process(new NamedState())
            .uid("e04-c6-uid-contract")  // 升级时此 uid 必须稳定
            .print();
        env.execute("e04-c6-uid-contract-demo");
    }

    public static final class NamedState extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<String> lastPage;

        @Override
        public void open(OpenContext ctx) {
            // 状态名同样是契约的一部分
            lastPage = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-page-v1", String.class));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            String prev = lastPage.value();
            lastPage.update(e.page);
            out.collect("UID-STATE user=%s prev=%s now=%s"
                    .formatted(e.userId, prev == null ? "-" : prev, e.page));
        }
    }
}
