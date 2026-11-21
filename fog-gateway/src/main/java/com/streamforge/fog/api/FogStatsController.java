package com.streamforge.fog.api;

import com.streamforge.fog.adaptive.AdaptiveCompressionSelector;
import com.streamforge.fog.grpc.CloudGrpcForwarder;
import com.streamforge.fog.mqtt.FogMqttConsumer;
import com.streamforge.fog.wal.DiskWriteAheadLog;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.LinkedHashMap;

@RestController
@RequestMapping("/api/fog")
public class FogStatsController {
    private final FogMqttConsumer mqttConsumer;
    private final CloudGrpcForwarder forwarder;
    private final DiskWriteAheadLog wal;
    private final AdaptiveCompressionSelector selector;

    public FogStatsController(FogMqttConsumer mqttConsumer,
                              CloudGrpcForwarder forwarder,
                              DiskWriteAheadLog wal,
                              AdaptiveCompressionSelector selector) {
        this.mqttConsumer = mqttConsumer;
        this.forwarder = forwarder;
        this.wal = wal;
        this.selector = selector;
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        long bytesSaved = forwarder.getBytesSaved();
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("ingressRateEventsPerSec", selector.getIngressRate());
        stats.put("batchesForwarded", forwarder.getBatchesSent());
        stats.put("rawBytes", forwarder.getRawBytes());
        stats.put("compressedBytes", forwarder.getCompressedBytes());
        stats.put("bytesSaved", bytesSaved);
        stats.put("cumulativeCompressionRatio",
                forwarder.getRawBytes() == 0
                        ? 1.0
                        : (double) forwarder.getCompressedBytes()
                        / forwarder.getRawBytes());
        stats.put("activeEncodingCompression", forwarder.getActiveCodec());
        stats.put("circuitBreakerState", forwarder.getCircuitBreakerState());
        stats.put("pendingWalSegmentCount", wal.getPendingSegmentCount());
        stats.put("pendingWalBytes", wal.getPendingBytes());
        stats.put("ingestionQueueSize", mqttConsumer.getQueueSize());
        return stats;
    }

    @PostMapping("/compression-override")
    public Map<String, String> compressionOverride(
            @RequestBody CompressionOverrideRequest request) {
        selector.setOverrideMode(request.mode());
        return Map.of("mode", selector.getOverrideMode());
    }

    public record CompressionOverrideRequest(String mode) {
    }
}
