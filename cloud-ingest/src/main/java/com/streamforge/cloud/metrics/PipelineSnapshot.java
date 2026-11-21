package com.streamforge.cloud.metrics;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record PipelineSnapshot(
        long batchesIngested,
        long sensorEventsPersisted,
        long duplicateBatchesReplayed,
        long checksumFailures,
        long rawBytes,
        long compressedBytes,
        double compressionRatio,
        List<Map<String, Object>> latestBatch,
        Instant timestamp) {
}
