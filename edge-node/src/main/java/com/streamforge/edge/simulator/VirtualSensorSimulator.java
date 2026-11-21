package com.streamforge.edge.simulator;

import com.streamforge.edge.config.EdgeProperties;
import com.streamforge.edge.filter.AdaptiveFilterChain;
import com.streamforge.edge.mqtt.EdgeMqttPublisher;
import com.streamforge.proto.SensorEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class VirtualSensorSimulator {
    private final EdgeProperties properties;
    private final EdgeMqttPublisher publisher;
    private final AdaptiveFilterChain filterChain;
    private final ExecutorService executor =
            Executors.newVirtualThreadPerTaskExecutor();
    private final Map<Integer, AtomicBoolean> workerRunning =
            new ConcurrentHashMap<>();
    private final AtomicInteger activeNodeCount = new AtomicInteger();
    private final AtomicInteger anomalyBurstsRemaining = new AtomicInteger();
    private volatile boolean stopped;

    public VirtualSensorSimulator(EdgeProperties properties,
                                  EdgeMqttPublisher publisher) {
        this.properties = properties;
        this.publisher = publisher;
        this.filterChain = new AdaptiveFilterChain(
                properties.getZscoreThreshold(),
                properties.getDeadbandEpsilon(),
                properties.getMaxHeartbeatMs());
    }

    @PostConstruct
    public void start() {
        setNodeCount(properties.getNodeCount());
    }

    public void setNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("nodeCount must be at least 1");
        }
        properties.setNodeCount(nodeCount);
        activeNodeCount.set(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            workerRunning.computeIfAbsent(i, key -> {
                AtomicBoolean running = new AtomicBoolean(true);
                executor.submit(() -> runNode(key));
                return running;
            });
        }
    }

    public void setEmitIntervalMs(int emitIntervalMs) {
        if (emitIntervalMs < 1) {
            throw new IllegalArgumentException("emitIntervalMs must be at least 1");
        }
        properties.setEmitIntervalMs(emitIntervalMs);
    }

    public void setDeadbandEpsilon(double deadbandEpsilon) {
        properties.setDeadbandEpsilon(deadbandEpsilon);
        filterChain.updateThresholds(
                properties.getZscoreThreshold(),
                deadbandEpsilon,
                properties.getMaxHeartbeatMs());
    }

    public void setZscoreThreshold(double zscoreThreshold) {
        properties.setZscoreThreshold(zscoreThreshold);
        filterChain.updateThresholds(
                zscoreThreshold,
                properties.getDeadbandEpsilon(),
                properties.getMaxHeartbeatMs());
    }

    public void triggerAnomalyBurst(int sampleCount) {
        anomalyBurstsRemaining.set(Math.max(1, Math.min(sampleCount, 100)));
    }

    private void runNode(int nodeIndex) {
        Random random = new Random(0x5F3759DL ^ nodeIndex);
        long sequence = 0;
        while (!stopped && workerRunning.get(nodeIndex).get()) {
            int nodeCount = properties.getNodeCount();
            if (nodeIndex >= nodeCount) {
                sleep(properties.getEmitIntervalMs());
                continue;
            }

            long timestamp = System.currentTimeMillis();
            boolean burst = anomalyBurstsRemaining.getAndUpdate(
                    value -> value > 0 ? value - 1 : 0) > 0;

            emitSensor(nodeIndex, "temperature",
                    22.0 + 3.0 * Math.sin(sequence / 18.0)
                            + random.nextGaussian() * 0.04,
                    timestamp, burst ? 45.0 : 0.0);
            emitSensor(nodeIndex, "vibration",
                    8.0 + 4.0 * Math.sin(sequence / 11.0)
                            + random.nextGaussian() * 0.08,
                    timestamp, burst ? 60.0 : 0.0);
            emitSensor(nodeIndex, "pressure",
                    1013.0 + 12.0 * Math.sin(sequence / 25.0)
                            + random.nextGaussian() * 0.15,
                    timestamp, burst ? 900.0 : 0.0);

            sequence++;
            sleep(properties.getEmitIntervalMs());
        }
    }

    private void emitSensor(int nodeIndex, String sensorType, double baseValue,
                            long timestamp, double spike) {
        SensorEvent event = SensorEvent.newBuilder()
                .setNodeId("edge-" + String.format("%03d", nodeIndex + 1))
                .setSensorType(sensorType)
                .setTimestampMs(timestamp)
                .setValue(baseValue + spike)
                .setAnomaly(false)
                .build();

        SensorEvent forwarded = filterChain.evaluate(event);
        if (forwarded != null) {
            publisher.publish(forwarded);
        }
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public EdgeStats getStats() {
        long generated = filterChain.getTotalGenerated();
        long forwarded = filterChain.getTotalForwarded();
        double reduction = generated == 0
                ? 0.0
                : 100.0 * (generated - forwarded) / generated;
        return new EdgeStats(
                generated,
                forwarded,
                generated - forwarded,
                filterChain.getDroppedSanity(),
                filterChain.getDroppedDeadband(),
                filterChain.getAnomaliesDetected(),
                reduction,
                publisher.getRingBufferSize(),
                publisher.getRingBufferEvictedCount(),
                activeNodeCount.get(),
                properties.getDeadbandEpsilon(),
                properties.getZscoreThreshold(),
                properties.getEmitIntervalMs(),
                properties.getMaxHeartbeatMs());
    }

    @PreDestroy
    public void close() {
        stopped = true;
        executor.shutdownNow();
    }

    public AdaptiveFilterChain getFilterChain() {
        return filterChain;
    }

    public record EdgeStats(
            long generated,
            long forwarded,
            long dropped,
            long droppedSanity,
            long droppedDeadband,
            long anomaliesDetected,
            double reductionPercent,
            int ringBufferSize,
            long ringBufferEvictedCount,
            int activeNodeCount,
            double deadbandEpsilon,
            double zscoreThreshold,
            int emitIntervalMs,
            long maxHeartbeatMs) {
    }
}
