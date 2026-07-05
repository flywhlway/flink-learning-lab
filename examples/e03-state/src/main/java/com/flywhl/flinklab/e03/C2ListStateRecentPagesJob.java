package com.flywhl.flinklab.e03;

import com.flywhl.flinklab.common.Event;
import com.flywhl.flinklab.common.Labs;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * e03-C2 · ListState:维护每用户最近 5 次访问轨迹(行为序列特征的原型)。
 *
 * <p>要点:ListState 没有"定长"能力,截断逻辑自己写;RocksDB 下 add() 是 O(1) 追加,
 * 但 get() 反序列化整个 list —— 高频全量读 + 截断的组合在大 list 上会成为瓶颈,
 * 生产上超过百级元素应改 MapState<index, T> 环形结构(见模块 README 讨论)。
 */
public final class C2ListStateRecentPagesJob {
    private C2ListStateRecentPagesJob() {
    }

    private static final int KEEP = 5;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Labs.events(env, "trail", 20, 4, 8, 500)
            .keyBy(e -> e.userId)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private transient ListState<String> recent;

                @Override
                public void open(OpenContext ctx) {
                    recent = getRuntimeContext().getListState(
                            new ListStateDescriptor<>("recent-pages", String.class));
                }

                @Override
                public void processElement(Event e, Context ctx, Collector<String> out)
                        throws Exception {
                    List<String> pages = new ArrayList<>();
                    for (String p : recent.get()) {
                        pages.add(p);
                    }
                    pages.add(e.page);
                    if (pages.size() > KEEP) {
                        pages = pages.subList(pages.size() - KEEP, pages.size());
                    }
                    recent.update(pages);
                    out.collect("user=%s trail=%s".formatted(e.userId, pages));
                }
            })
            .uid("e03-c2-trail")
            .print();

        env.execute("e03-c2-list-state-recent-pages");
    }
}
