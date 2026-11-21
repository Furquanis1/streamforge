package com.streamforge.cloud.metrics;

import com.streamforge.cloud.persistence.TelemetryBatchEntity;
import com.streamforge.cloud.persistence.TelemetryBatchRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class CloudMetricsTracker {
    private final TelemetryBatchRepository batchRepository;
    private final CopyOnWriteArrayList<SseEmitter> emitters =
            new CopyOnWriteArrayList<>();
    private final AtomicLong batchesIngested = new AtomicLong();
    private final AtomicLong sensorEventsPersisted = new AtomicLong();
    private final AtomicLong duplicateBatchesReplayed = new AtomicLong();
    private final AtomicLong checksumFailures = new AtomicLong();
    private final AtomicLong rawBytes = new AtomicLong();
    private final AtomicLong compressedBytes = new AtomicLong();
    private final List<TelemetryBatchEntity> recentHistory =
            new CopyOnWriteArrayList<>();

    public CloudMetricsTracker(TelemetryBatchRepository batchRepository) {
        this.batchRepository = batchRepository;
    }

    public void recordAccepted(TelemetryBatchEntity batch) {
        batchesIngested.incrementAndGet();
        sensorEventsPersisted.addAndGet(batch.getEventCount());
        rawBytes.addAndGet(batch.getUncompressedBytes());
        compressedBytes.addAndGet(batch.getCompressedBytes());
        recentHistory.add(0, batch);
        while (recentHistory.size() > 25) {
            recentHistory.remove(recentHistory.size() - 1);
        }
    }

    public void recordDuplicate() {
        duplicateBatchesReplayed.incrementAndGet();
    }

    public void recordChecksumFailure() {
        checksumFailures.incrementAndGet();
    }

    public SseEmitter subscribe(long heartbeatMs) {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(error -> emitters.remove(emitter));
        return emitter;
    }

    @Scheduled(fixedDelayString = "${streamforge.cloud.sse-heartbeat-ms:2000}")
    public void broadcastSnapshot() {
        PipelineSnapshot snapshot = snapshot();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("pipeline")
                        .data(snapshot));
            } catch (IOException | IllegalStateException e) {
                emitters.remove(emitter);
            }
        }
    }

    public PipelineSnapshot snapshot() {
        List<Map<String, Object>> latest = new ArrayList<>();
        for (TelemetryBatchEntity batch : recentHistory) {
            latest.add(Map.of(
                    "batchId", batch.getBatchId(),
                    "gatewayId", batch.getGatewayId(),
                    "encodingType", batch.getEncodingType(),
                    "compressionType", batch.getCompressionType(),
                    "eventCount", batch.getEventCount(),
                    "compressedBytes", batch.getCompressedBytes()));
        }
        long raw = rawBytes.get();
        long compressed = compressedBytes.get();
        return new PipelineSnapshot(
                batchesIngested.get(),
                sensorEventsPersisted.get(),
                duplicateBatchesReplayed.get(),
                checksumFailures.get(),
                raw,
                compressed,
                raw == 0 ? 1.0 : (double) raw / compressed,
                latest,
                Instant.now());
    }

    public List<TelemetryBatchEntity> getRecentHistory() {
        if (!recentHistory.isEmpty()) {
            return new ArrayList<>(recentHistory);
        }
        return batchRepository.findTop25ByOrderByIngestedAtDesc();
    }

    public long getBatchesIngested() { return batchesIngested.get(); }
    public long getSensorEventsPersisted() { return sensorEventsPersisted.get(); }
    public long getDuplicateBatchesReplayed() { return duplicateBatchesReplayed.get(); }
    public long getChecksumFailures() { return checksumFailures.get(); }
    public long getRawBytes() { return rawBytes.get(); }
    public long getCompressedBytes() { return compressedBytes.get(); }
}
