package com.streamforge.edge.filter;

import com.streamforge.proto.SensorEvent;

public final class ZScoreAnomalyFilter implements EdgeFilter {
    private volatile double threshold;

    public ZScoreAnomalyFilter(double threshold) {
        if (threshold < 0.0) {
            throw new IllegalArgumentException("Z-score threshold must not be negative");
        }
        this.threshold = threshold;
    }

    public void setThreshold(double threshold) {
        if (threshold < 0.0) {
            throw new IllegalArgumentException("Z-score threshold must not be negative");
        }
        this.threshold = threshold;
    }

    @Override
    public FilterResult evaluate(SensorEvent event, SensorStreamState state) {
        double zScore = state.zScore(event.getValue());
        if (state.getCount() >= 10 && Math.abs(zScore) >= threshold) {
            return new FilterResult(true, "anomaly_zscore");
        }
        return new FilterResult(true, "zscore_ok");
    }
}
