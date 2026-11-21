package com.streamforge.cloud.idempotency;

import com.streamforge.cloud.config.CloudProperties;
import com.streamforge.cloud.persistence.TelemetryBatchRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class BatchIdempotencyService {
    private final StringRedisTemplate redisTemplate;
    private final TelemetryBatchRepository batchRepository;
    private final CloudProperties properties;

    public BatchIdempotencyService(StringRedisTemplate redisTemplate,
                                   TelemetryBatchRepository batchRepository,
                                   CloudProperties properties) {
        this.redisTemplate = redisTemplate;
        this.batchRepository = batchRepository;
        this.properties = properties;
    }

    public boolean tryAcquire(String batchId) {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        try {
            Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                    "streamforge:batch:" + batchId,
                    "1",
                    Duration.ofSeconds(properties.getIdempotencyTtlSeconds()));
            return Boolean.TRUE.equals(acquired);
        } catch (RuntimeException redisUnavailable) {
            return !batchRepository.existsById(batchId);
        }
    }
}
