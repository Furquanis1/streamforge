package com.streamforge.edge.filter;

import com.streamforge.proto.SensorEvent;

public interface EdgeFilter {
    FilterResult evaluate(SensorEvent event, SensorStreamState state);

    record FilterResult(boolean forward, String reason) {
    }
}
