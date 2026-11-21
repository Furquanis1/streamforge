package com.streamforge.edge.filter;

public final class SensorStreamState {
    private double lastForwardedValue;
    private long lastForwardedTimestamp;
    private boolean hasForwarded;
    private long count;
    private double mean;
    private double m2;

    public double getLastForwardedValue() {
        return lastForwardedValue;
    }

    public void setLastForwardedValue(double lastForwardedValue) {
        this.lastForwardedValue = lastForwardedValue;
    }

    public long getLastForwardedTimestamp() {
        return lastForwardedTimestamp;
    }

    public void setLastForwardedTimestamp(long lastForwardedTimestamp) {
        this.lastForwardedTimestamp = lastForwardedTimestamp;
    }

    public boolean isHasForwarded() {
        return hasForwarded;
    }

    public void setHasForwarded(boolean hasForwarded) {
        this.hasForwarded = hasForwarded;
    }

    public long getCount() {
        return count;
    }

    public double getMean() {
        return mean;
    }

    public double getVariance() {
        return count > 1 ? m2 / (count - 1) : 0.0;
    }

    public void updateWelford(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
    }

    public double zScore(double value) {
        double variance = getVariance();
        if (count < 10 || variance <= 0.0) {
            return 0.0;
        }
        return (value - mean) / Math.sqrt(variance);
    }
}
