package com.flywhl.flinklab.e02.support;

import com.flywhl.flinklab.common.Event;
import org.apache.flink.api.common.functions.AggregateFunction;

/** e02 通用计数增量聚合。 */
public final class CountAgg implements AggregateFunction<Event, Long, Long> {
    @Override public Long createAccumulator()          { return 0L; }
    @Override public Long add(Event e, Long acc)       { return acc + 1; }
    @Override public Long getResult(Long acc)          { return acc; }
    @Override public Long merge(Long a, Long b)        { return a + b; }
}
