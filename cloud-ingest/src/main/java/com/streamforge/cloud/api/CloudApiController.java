package com.streamforge.cloud.api;

import com.streamforge.cloud.chaos.ChaosController;
import com.streamforge.cloud.config.CloudProperties;
import com.streamforge.cloud.metrics.CloudMetricsTracker;
import com.streamforge.cloud.persistence.SensorReadingEntity;
import com.streamforge.cloud.persistence.SensorReadingRepository;
import com.streamforge.cloud.persistence.TelemetryBatchEntity;
import com.streamforge.cloud.persistence.TelemetryBatchRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cloud")
@CrossOrigin(origins = "*")
public class CloudApiController {
    private final TelemetryBatchRepository batchRepository;
    private final SensorReadingRepository readingRepository;
    private final CloudMetricsTracker metricsTracker;
    private final CloudProperties properties;
    private final ChaosController chaosController;

    public CloudApiController(TelemetryBatchRepository batchRepository,
                              SensorReadingRepository readingRepository,
                              CloudMetricsTracker metricsTracker,
                              CloudProperties properties,
                              ChaosController chaosController) {
        this.batchRepository = batchRepository;
        this.readingRepository = readingRepository;
        this.metricsTracker = metricsTracker;
        this.properties = properties;
        this.chaosController = chaosController;
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("batchesIngested", metricsTracker.getBatchesIngested());
        result.put("sensorEventsPersisted", metricsTracker.getSensorEventsPersisted());
        result.put("duplicateBatchesReplayed",
                metricsTracker.getDuplicateBatchesReplayed());
        result.put("checksumErrors", metricsTracker.getChecksumFailures());
        result.put("rawBytes", metricsTracker.getRawBytes());
        result.put("compressedBytes", metricsTracker.getCompressedBytes());
        result.put("compressionRatio",
                metricsTracker.getRawBytes() == 0
                        ? 1.0
                        : (double) metricsTracker.getRawBytes()
                        / metricsTracker.getCompressedBytes());
        result.put("recentBatches", metricsTracker.getRecentHistory());
        return result;
    }

    @GetMapping("/readings/recent")
    public List<SensorReadingEntity> recentReadings(
            @RequestParam(defaultValue = "100") int limit) {
        int boundedLimit = Math.max(1, Math.min(limit, 1000));
        return readingRepository.findRecent(PageRequest.of(0, boundedLimit));
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return metricsTracker.subscribe(properties.getSseHeartbeatMs());
    }

    @PostMapping("/chaos")
    public Map<String, Object> chaos(@RequestBody ChaosRequest request) {
        if (request.failMode() != null) {
            chaosController.setFailMode(request.failMode());
        }
        if (request.latencyMs() != null) {
            chaosController.setLatencyMs(request.latencyMs());
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("failMode", chaosController.isFailMode());
        result.put("latencyMs", chaosController.getLatencyMs());
        return result;
    }

    public record ChaosRequest(Boolean failMode, Integer latencyMs) {
    }
}
