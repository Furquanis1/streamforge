package com.streamforge.cloud.grpc;

import com.streamforge.cloud.chaos.ChaosController;
import com.streamforge.cloud.idempotency.BatchIdempotencyService;
import com.streamforge.cloud.metrics.CloudMetricsTracker;
import com.streamforge.cloud.persistence.TelemetryBatchEntity;
import com.streamforge.cloud.persistence.TelemetryPersistenceService;
import com.streamforge.common.compression.CompressionFactory;
import com.streamforge.common.compression.CompressionStrategy;
import com.streamforge.common.integrity.ChecksumUtil;
import com.streamforge.common.codec.BatchCodecFactory;
import com.streamforge.proto.BatchAckResponse;
import com.streamforge.proto.CompressedBatchRequest;
import com.streamforge.proto.SensorEvent;
import com.streamforge.proto.TelemetryIngestServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class TelemetryIngestGrpcService
        extends TelemetryIngestServiceGrpc.TelemetryIngestServiceImplBase {
    private final BatchIdempotencyService idempotencyService;
    private final TelemetryPersistenceService persistenceService;
    private final CloudMetricsTracker metricsTracker;
    private final ChaosController chaosController;

    public TelemetryIngestGrpcService(
            BatchIdempotencyService idempotencyService,
            TelemetryPersistenceService persistenceService,
            CloudMetricsTracker metricsTracker,
            ChaosController chaosController) {
        this.idempotencyService = idempotencyService;
        this.persistenceService = persistenceService;
        this.metricsTracker = metricsTracker;
        this.chaosController = chaosController;
    }

    @Override
    public void ingestBatch(CompressedBatchRequest request,
                            StreamObserver<BatchAckResponse> responseObserver) {
        if (chaosController.isFailMode()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Simulated cloud outage")
                    .asRuntimeException());
            return;
        }
        int latency = chaosController.getLatencyMs();
        if (latency > 0) {
            try {
                Thread.sleep(Math.min(latency, 30_000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        byte[] payload = request.getPayload().toByteArray();
        if (!ChecksumUtil.verifyCrc32(payload, request.getCrc32Checksum())) {
            metricsTracker.recordChecksumFailure();
            responseObserver.onNext(BatchAckResponse.newBuilder()
                    .setBatchId(request.getBatchId())
                    .setAccepted(false)
                    .setDuplicate(false)
                    .setServerReceivedAtMs(System.currentTimeMillis())
                    .setMessage("CRC32 checksum mismatch")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        if (!idempotencyService.tryAcquire(request.getBatchId())) {
            metricsTracker.recordDuplicate();
            responseObserver.onNext(BatchAckResponse.newBuilder()
                    .setBatchId(request.getBatchId())
                    .setAccepted(true)
                    .setDuplicate(true)
                    .setServerReceivedAtMs(System.currentTimeMillis())
                    .setMessage("Duplicate batch")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        try {
            CompressionStrategy compression =
                    CompressionFactory.get(request.getCompression());
            byte[] decompressed = compression.decompress(
                    payload, request.getRawBytesSize());
            List<SensorEvent> events =
                    BatchCodecFactory.get(request.getEncoding()).decode(decompressed);

            persistenceService.persist(request, events);
            TelemetryBatchEntity batch = new TelemetryBatchEntity();
            batch.setBatchId(request.getBatchId());
            batch.setGatewayId(request.getFogNodeId());
            batch.setEncodingType(request.getEncoding().name());
            batch.setCompressionType(request.getCompression().name());
            batch.setEventCount(events.size());
            batch.setUncompressedBytes(request.getRawBytesSize());
            batch.setCompressedBytes(request.getCompressedBytesSize());
            batch.setCrc32Checksum(request.getCrc32Checksum());
            batch.setBatchTimestampMs(request.getCreatedAtMs());
            batch.setIngestedAt(Instant.now());
            metricsTracker.recordAccepted(batch);

            responseObserver.onNext(BatchAckResponse.newBuilder()
                    .setBatchId(request.getBatchId())
                    .setAccepted(true)
                    .setDuplicate(false)
                    .setServerReceivedAtMs(System.currentTimeMillis())
                    .setMessage("Batch accepted")
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Batch processing failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
