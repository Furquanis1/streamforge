package com.streamforge.edge.filter;

import com.streamforge.proto.SensorEvent;

public final class RangeSanityFilter implements EdgeFilter {
    @Override
    public FilterResult evaluate(SensorEvent event, SensorStreamState state) {
        double value = event.getValue();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return new FilterResult(false, "non_finite");
        }

        boolean invalid = switch (event.getSensorType()) {
            case "temperature" -> value < -50.0 || value > 200.0;
            case "vibration" -> value < 0.0 || value > 100.0;
            case "pressure" -> value < 0.0 || value > 10000.0;
            default -> false;
        };
        return invalid
                ? new FilterResult(false, "range_sanity")
                : new FilterResult(true, "sanity_ok");
    }
}
