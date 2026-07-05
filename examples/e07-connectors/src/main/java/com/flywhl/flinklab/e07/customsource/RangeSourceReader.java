package com.flywhl.flinklab.e07.customsource;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.core.io.InputStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 分片读取器:运行于 TaskManager,负责"真正把数据读出来"。
 * pollNext 是主循环入口:有数据就 emit 并返回 MORE_AVAILABLE,
 * 分片读完返回 END_OF_INPUT——这是 FLIP-27 与老 SourceFunction 的本质区别:
 * 老接口是"我主动 run() 一个 while(true) 循环",新接口是"框架反复调我一次读一点",
 * 天然支持背压(框架控制调用节奏)与 checkpoint(状态是普通字段,直接快照)。
 */
public final class RangeSourceReader implements SourceReader<Long, RangeSplit> {

    private final List<RangeSplit> assigned = new ArrayList<>();

    @Override
    public void start() {
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Long> output) {
        if (assigned.isEmpty()) {
            return InputStatus.END_OF_INPUT;
        }
        RangeSplit split = assigned.get(0);
        if (split.current < split.end) {
            output.collect(split.current);
            split.current++;
            return InputStatus.MORE_AVAILABLE;
        }
        assigned.remove(0);
        return assigned.isEmpty() ? InputStatus.END_OF_INPUT : InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<RangeSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(assigned);   // current 字段随对象一起序列化,恢复即续读
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);   // 数字源永远就绪;IO 源在此返回未完成 future
    }

    @Override
    public void addSplits(List<RangeSplit> splits) {
        assigned.addAll(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public void close() {
    }
}
