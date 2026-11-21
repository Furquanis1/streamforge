package com.streamforge.edge.filter;

import com.streamforge.proto.SensorEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public final class AdaptiveFilterChain {
    private final RangeSanityFilter sanityFilter = new RangeSanityFilter();
    private final ZScoreAnomalyFilter zScoreFilter;
    private final DeadbandHeartbeatFilter deadbandFilter;
    private final ConcurrentMap<String, SensorStreamState> states = new ConcurrentHashMap<>();

    private final AtomicLong totalGenerated = new AtomicLong();
    private final AtomicLong totalForwarded = new AtomicLong();
    private final AtomicLong droppedSanity = new AtomicLong();
    private final AtomicLong droppedDeadband = new AtomicLong();
    private final AtomicLong anomaliesDetected = new AtomicLong();

    public AdaptiveFilterChain(double zscoreThreshold, double deadbandEpsilon,
                               long maxHeartbeatMs) {
        this.zScoreFilter = new ZScoreAnomalyFilter(zscoreThreshold);
        this.deadbandFilter = new DeadbandHeartbeatFilter(deadbandEpsilon, maxHeartbeatMs);
    }

    public SensorEvent evaluate(SensorEvent event) {
        totalGenerated.incrementAndGet();
        SensorStreamState state = states.computeIfAbsent(
                event.getNodeId() + "|" + event.getSensorType(),
                key -> new SensorStreamState());

        EdgeFilter.FilterResult sanity = sanityFilter.evaluate(event, state);
        if (!sanity.forward()) {
            droppedSanity.incrementAndGet();
            return null;
        }

        EdgeFilter.FilterResult zscore = zScoreFilter.evaluate(event, state);
        if (zscore.reason().equals("anomaly_zscore")) {
            anomaliesDetected.incrementAndGet();
            SensorEvent anomalous = event.toBuilder().setAnomaly(true).build();
            state.updateWelford(anomalous.getValue());
            deadbandFilter.evaluate(anomalous, state);
            totalForwarded.incrementAndGet();
            return anomalous;
        }

        if (event.getAnomaly()) {
            state.updateWelford(event.getValue());
            state.setLastForwardedValue(event.getValue());
            state.setLastForwardedTimestamp(event.getTimestampMs());
            state.setHasForwarded(true);
            totalForwarded.incrementAndGet();
            return event;
        }
        state.updateWelford(event.getValue());
        EdgeFilter.FilterResult deadband = deadbandFilter.evaluate(event, state);
        if (!deadband.forward()) {
            droppedDeadband.incrementAndGet();
            return null;
        }

        totalForwarded.incrementAndGet();
        return event;
    }

    public long getTotalGenerated() { return totalGenerated.get(); }
    public long getTotalForwarded() { return totalForwarded.get(); }
    public long getDroppedSanity() { return droppedSanity.get(); }
    public long getDroppedDeadband() { return droppedDeadband.get(); }
    public long getAnomaliesDetected() { return anomaliesDetected.get(); }

    public void updateThresholds(double zscoreThreshold, double deadbandEpsilon,
                                 long maxHeartbeatMs) {
        zScoreFilter.setThreshold(zscoreThreshold);
        deadbandFilter.setEpsilon(deadbandEpsilon);
        deadbandFilter.setMaxHeartbeatMs(maxHeartbeatMs);
    }

    public void resetStreamStates() {
        states.clear();
    }
}
