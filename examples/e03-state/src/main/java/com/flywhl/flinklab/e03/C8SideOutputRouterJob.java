package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * e03-C8 · Side Output:一次遍历的多路分流(主流/大额告警/脏数据死信)。
 *
 * <p>对比 filter 三连:三次遍历三份反序列化;ProcessFunction + side output
 * 一次遍历各走各路,且死信通道是"军规 6(禁止吞脏数据)"的落地形态 ——
 * 生产上 dirty 流接死信 topic,替代此处的 print。
 */
public final class C8SideOutputRouterJob {
    private C8SideOutputRouterJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final OutputTag<String> alerts = new OutputTag<>("alerts") {
        };
        final OutputTag<String> dirty = new OutputTag<>("dirty") {
        };

        SingleOutputStreamOperator<String> main =
            Labs.events(env, "mixed", 30, 8, 6, 500)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event e, Context ctx, Collector<String> out) {
                        // 模拟脏数据:约 1/12 的事件视为无法解析
                        if (Math.abs(e.userId.hashCode() + e.page.hashCode()) % 12 == 0) {
                            ctx.output(dirty, "DIRTY  " + e + "  → 死信topic对账");
                            return;
                        }
                        if (e.amount > 450) {
                            ctx.output(alerts, "ALERT  " + e);
                        }
                        out.collect("MAIN   " + e.page);
                    }
                })
                .uid("e03-c8-router");

        main.print();
        main.getSideOutput(alerts).print();
        main.getSideOutput(dirty).print();

        env.execute("e03-c8-side-output-router");
    }
}
