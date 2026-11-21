package com.streamforge.edge.filter;

import com.streamforge.proto.SensorEvent;

public final class DeadbandHeartbeatFilter implements EdgeFilter {
    private volatile double epsilon;
    private volatile long maxHeartbeatMs;

    public DeadbandHeartbeatFilter(double epsilon, long maxHeartbeatMs) {
        if (epsilon < 0.0) {
            throw new IllegalArgumentException("Deadband epsilon must not be negative");
        }
        if (maxHeartbeatMs < 0) {
            throw new IllegalArgumentException("Max heartbeat must not be negative");
        }
        this.epsilon = epsilon;
        this.maxHeartbeatMs = maxHeartbeatMs;
    }

    public void setEpsilon(double epsilon) {
        if (epsilon < 0.0) {
            throw new IllegalArgumentException("Deadband epsilon must not be negative");
        }
        this.epsilon = epsilon;
    }

    public void setMaxHeartbeatMs(long maxHeartbeatMs) {
        if (maxHeartbeatMs < 0) {
            throw new IllegalArgumentException("Max heartbeat must not be negative");
        }
        this.maxHeartbeatMs = maxHeartbeatMs;
    }

    @Override
    public FilterResult evaluate(SensorEvent event, SensorStreamState state) {
        if (!state.isHasForwarded()) {
            state.setHasForwarded(true);
            state.setLastForwardedValue(event.getValue());
            state.setLastForwardedTimestamp(event.getTimestampMs());
            return new FilterResult(true, "initial");
        }

        long elapsed = event.getTimestampMs() - state.getLastForwardedTimestamp();
        boolean heartbeat = elapsed >= maxHeartbeatMs;
        boolean changed = Math.abs(event.getValue() - state.getLastForwardedValue()) >= epsilon;
        if (!heartbeat && !changed) {
            return new FilterResult(false, "deadband");
        }

        state.setLastForwardedValue(event.getValue());
        state.setLastForwardedTimestamp(event.getTimestampMs());
        return new FilterResult(true, heartbeat ? "heartbeat" : "changed");
    }
}
