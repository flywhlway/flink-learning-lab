package com.flywhl.flinklab.e07.customsource;

import org.apache.flink.api.connector.source.SourceSplit;

/** 一个数字区间分片:[start, end)。 */
public final class RangeSplit implements SourceSplit {
    public final int id;
    public long current;
    public final long end;

    public RangeSplit(int id, long start, long end) {
        this.id = id;
        this.current = start;
        this.end = end;
    }

    @Override
    public String splitId() {
        return "range-" + id;
    }
}
