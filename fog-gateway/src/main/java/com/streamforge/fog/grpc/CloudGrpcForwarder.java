package com.streamforge.fog.grpc;

import com.streamforge.fog.config.FogProperties;
import com.streamforge.fog.wal.DiskWriteAheadLog;
import com.streamforge.proto.BatchAckResponse;
import com.streamforge.proto.CompressedBatchRequest;
import com.streamforge.proto.TelemetryIngestServiceGrpc;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class CloudGrpcForwarder {
    static final class BatchRejectedException extends RuntimeException {
        BatchRejectedException(String message) {
            super(message);
        }
    }

    private final ManagedChannel channel;
    private final TelemetryIngestServiceGrpc.TelemetryIngestServiceBlockingStub stub;
    private final DiskWriteAheadLog wal;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final AtomicLong batchesSent = new AtomicLong();
    private final AtomicLong bytesSaved = new AtomicLong();
    private final AtomicLong rawBytes = new AtomicLong();
    private final AtomicLong compressedBytes = new AtomicLong();
    private final AtomicReference<String> activeCodec =
            new AtomicReference<>("AUTO");

    @Autowired
    public CloudGrpcForwarder(FogProperties properties, DiskWriteAheadLog wal) {
        this.wal = wal;
        this.channel = ManagedChannelBuilder
                .forAddress(properties.getCloudGrpcHost(), properties.getCloudGrpcPort())
                .usePlaintext()
                .build();
        this.stub = TelemetryIngestServiceGrpc.newBlockingStub(channel);
        this.circuitBreaker = createCircuitBreaker();
        this.retry = createRetry();
    }

    public CloudGrpcForwarder(TelemetryIngestServiceGrpc.TelemetryIngestServiceBlockingStub stub,
                             DiskWriteAheadLog wal) {
        this.channel = null;
        this.stub = stub;
        this.wal = wal;
        this.circuitBreaker = createCircuitBreaker();
        this.retry = createRetry();
    }

    private static CircuitBreaker createCircuitBreaker() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(4)
                .minimumNumberOfCalls(1)
                .waitDurationInOpenState(Duration.ofSeconds(4))
                .permittedNumberOfCallsInHalfOpenState(2)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(StatusRuntimeException.class,
                        BatchRejectedException.class)
                .build();
        return CircuitBreakerRegistry.of(config).circuitBreaker("fog-cloud");
    }

    private static Retry createRetry() {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(200))
                .build();
        return RetryRegistry.of(config).retry("fog-cloud");
    }

    public BatchAckResponse forward(CompressedBatchRequest batch) {
        if (batch == null) {
            throw new IllegalArgumentException("batch must not be null");
        }
        try {
            BatchAckResponse response = circuitBreaker.executeSupplier(
                    Retry.decorateSupplier(retry, () -> {
                        BatchAckResponse ack = stub.ingestBatch(batch);
                        if (!ack.getAccepted()) {
                            throw new BatchRejectedException(
                                    ack.getMessage() == null
                                            ? "Cloud rejected batch"
                                            : ack.getMessage());
                        }
                        return ack;
                    }));
            if (response.getAccepted()) {
                batchesSent.incrementAndGet();
                rawBytes.addAndGet(batch.getRawBytesSize());
                compressedBytes.addAndGet(batch.getCompressedBytesSize());
                bytesSaved.addAndGet(
                        Math.max(0L, batch.getRawBytesSize()
                                - batch.getCompressedBytesSize()));
                activeCodec.set(batch.getEncoding() + "/" + batch.getCompression());
                return response;
            }
            throw new BatchRejectedException(
                    response.getMessage() == null
                            ? "Cloud rejected batch"
                            : response.getMessage());
        } catch (StatusRuntimeException | CallNotPermittedException e) {
            wal.append(batch);
            throw e;
        } catch (RuntimeException e) {
            wal.append(batch);
            throw e;
        }
    }

    public BatchAckResponse forwardBatch(CompressedBatchRequest batch) {
        return forward(batch);
    }

    public void forwardOrSpill(CompressedBatchRequest batch) {
        try {
            forward(batch);
        } catch (RuntimeException ignored) {
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void replayWal() {
        CircuitBreaker.State state = circuitBreaker.getState();
        if (state == CircuitBreaker.State.OPEN) {
            return;
        }
        if (wal.getPendingSegmentCount() == 0) {
            return;
        }
        List<CompressedBatchRequest> pending = wal.drainPendingBatches(100);
        for (CompressedBatchRequest batch : pending) {
            try {
                BatchAckResponse response = circuitBreaker.executeSupplier(
                        () -> {
                            BatchAckResponse ack = stub.ingestBatch(batch);
                            if (!ack.getAccepted()) {
                                throw new BatchRejectedException(
                                        ack.getMessage() == null
                                                ? "Cloud rejected replayed batch"
                                                : ack.getMessage());
                            }
                            return ack;
                        });
                if (response.getAccepted()) {
                    wal.acknowledgeUpTo(batch);
                    batchesSent.incrementAndGet();
                    rawBytes.addAndGet(batch.getRawBytesSize());
                    compressedBytes.addAndGet(batch.getCompressedBytesSize());
                    bytesSaved.addAndGet(
                            Math.max(0L, batch.getRawBytesSize()
                                    - batch.getCompressedBytesSize()));
                    activeCodec.set(batch.getEncoding() + "/" + batch.getCompression());
                }
            } catch (RuntimeException e) {
                return;
            }
        }
    }

    public String getCircuitBreakerState() {
        return circuitBreaker.getState().name();
    }

    public long getBatchesSent() {
        return batchesSent.get();
    }

    public long getBytesSaved() {
        return bytesSaved.get();
    }

    public long getRawBytes() {
        return rawBytes.get();
    }

    public long getCompressedBytes() {
        return compressedBytes.get();
    }

    public String getActiveCodec() {
        return activeCodec.get();
    }

    @PreDestroy
    public void close() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }
}
