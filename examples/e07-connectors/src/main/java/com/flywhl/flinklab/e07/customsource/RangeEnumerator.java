package com.flywhl.flinklab.e07.customsource;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * 分片枚举器:运行于 JobManager,负责"有哪些分片、分给谁"。
 * 本例把 [0, total) 均分成 numSplits 段,新 reader 上线即分配一段;
 * 分片分完后调用 signalNoMoreSplits 让该 reader 知道"再也没有新分片了"。
 */
public final class RangeEnumerator implements SplitEnumerator<RangeSplit, List<RangeSplit>> {

    private final SplitEnumeratorContext<RangeSplit> context;
    private final Deque<RangeSplit> pending;

    public RangeEnumerator(SplitEnumeratorContext<RangeSplit> context,
                           long total, int numSplits, List<RangeSplit> restored) {
        this.context = context;
        this.pending = new ArrayDeque<>();
        if (restored != null) {
            pending.addAll(restored);           // 从 checkpoint 恢复未分配完的分片
        } else {
            long step = total / numSplits;
            for (int i = 0; i < numSplits; i++) {
                long start = i * step;
                long end = (i == numSplits - 1) ? total : start + step;
                pending.add(new RangeSplit(i, start, end));
            }
        }
    }

    @Override
    public void start() {
        // 无需周期发现(有界数字源);Kafka 这类源会在此启动分区发现定时任务
    }

    @Override
    public void handleSplitRequest(int subtaskId, String hostname) {
        RangeSplit split = pending.poll();
        if (split != null) {
            context.assignSplit(split, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<RangeSplit> splits, int subtaskId) {
        pending.addAll(splits);   // reader 失败:分片退回待分配池,别的 reader 会来要
    }

    @Override
    public void addReader(int subtaskId) {
        // 新 reader 上线不主动推送;等它 handleSplitRequest 来要(pull 模型)
    }

    @Override
    public List<RangeSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pending);   // 只需快照"还没分出去的";已分配的由各 reader 自行快照
    }

    @Override
    public void close() {
    }
}
