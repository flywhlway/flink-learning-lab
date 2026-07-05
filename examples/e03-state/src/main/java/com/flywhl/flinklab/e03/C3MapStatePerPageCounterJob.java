package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * e03-C3 · MapState:key=用户,map=页面→次数(嵌套维度的正确姿势)。
 *
 * <p>对比错误姿势:ValueState&lt;HashMap&gt; —— 每次读写序列化整个 map;
 * MapState 在 RocksDB 下每个 entry 是独立 KV,点读点写 O(1),这是二者的本质差距。
 * 每处理 30 条输出一次该用户的 top 页面。
 */
public final class C3MapStatePerPageCounterJob {
    private C3MapStatePerPageCounterJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "pv", 40, 4, 10, 500)
            .keyBy(e -> e.userId)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private transient MapState<String, Long> pageCnt;
                private transient MapState<String, Long> meta; // 复用 MapState 存本 key 的处理条数

                @Override
                public void open(OpenContext ctx) {
                    pageCnt = getRuntimeContext().getMapState(
                            new MapStateDescriptor<>("page-cnt", String.class, Long.class));
                    meta = getRuntimeContext().getMapState(
                            new MapStateDescriptor<>("meta", String.class, Long.class));
                }

                @Override
                public void processElement(Event e, Context ctx, Collector<String> out)
                        throws Exception {
                    long c = pageCnt.contains(e.page) ? pageCnt.get(e.page) : 0;
                    pageCnt.put(e.page, c + 1);

                    long seen = (meta.contains("seen") ? meta.get("seen") : 0) + 1;
                    meta.put("seen", seen);
                    if (seen % 30 == 0) {
                        Map<String, Long> snap = new HashMap<>();
                        for (Map.Entry<String, Long> en : pageCnt.entries()) {
                            snap.put(en.getKey(), en.getValue());
                        }
                        out.collect("user=%s seen=%d dist=%s".formatted(ctx.getCurrentKey(), seen, snap));
                    }
                }
            })
            .uid("e03-c3-per-page")
            .print();

        env.execute("e03-c3-map-state-per-page");
    }
}
