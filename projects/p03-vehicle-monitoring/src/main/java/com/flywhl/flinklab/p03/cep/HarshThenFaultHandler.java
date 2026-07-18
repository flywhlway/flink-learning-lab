package com.flywhl.flinklab.p03.cep;

import com.flywhl.flinklab.p03.model.AlertEvent;
import org.apache.flink.util.OutputTag;

/**
 * {@link HarshThenFaultPattern} 的 MATCH/TIMEOUT Handler（patternId={@link PatternIds#HARSH_THEN_FAULT}）。
 */
public final class HarshThenFaultHandler extends AlertPatternHandler {

    public HarshThenFaultHandler() {
        super();
    }

    public HarshThenFaultHandler(OutputTag<AlertEvent> timeoutTag) {
        super(timeoutTag);
    }
}
