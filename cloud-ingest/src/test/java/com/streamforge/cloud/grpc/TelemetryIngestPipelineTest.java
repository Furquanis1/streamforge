package com.streamforge.cloud.grpc;

import com.streamforge.cloud.chaos.ChaosController;
import com.streamforge.cloud.idempotency.BatchIdempotencyService;
import com.streamforge.cloud.metrics.CloudMetricsTracker;
import com.streamforge.cloud.persistence.TelemetryBatchRepository;
import com.streamforge.cloud.persistence.TelemetryPersistenceService;
import com.streamforge.common.codec.BatchCodec;
import com.streamforge.common.codec.BatchCodecFactory;
import com.streamforge.common.compression.CompressionFactory;
import com.streamforge.common.compression.CompressionStrategy;
import com.streamforge.common.integrity.ChecksumUtil;
import com.streamforge.proto.BatchAckResponse;
import com.streamforge.proto.CompressedBatchRequest;
import com.streamforge.proto.CompressionType;
import com.streamforge.proto.EncodingType;
import com.streamforge.proto.SensorEvent;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

class TelemetryIngestPipelineTest {
    @Test
    void endToEndColumnarZstdValidationAndDuplicateHandling() {
        List<SensorEvent> events = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            events.add(SensorEvent.newBuilder()
                    .setNodeId("node-a")
                    .setSensorType("temperature")
                    .setTimestampMs(1_700_000_000_000L + i * 100L)
                    .setValue(25.0 + i * 0.01)
                    .setAnomaly(i == 499)
                    .build());
        }

        BatchCodec codec = BatchCodecFactory.get(EncodingType.COLUMNAR_DELTA);
        byte[] encoded = codec.encode(events);
        CompressionStrategy zstd = CompressionFactory.get(CompressionType.ZSTD);
        byte[] wirePayload = zstd.compress(encoded);

        CompressedBatchRequest valid = CompressedBatchRequest.newBuilder()
                .setBatchId("550e8400-e29b-41d4-a716-446655440000")
                .setFogNodeId("fog-test")
                .setCreatedAtMs(System.currentTimeMillis())
                .setRecordCount(events.size())
                .setRawBytesSize(encoded.length)
                .setCompressedBytesSize(wirePayload.length)
                .setCompression(CompressionType.ZSTD)
                .setEncoding(EncodingType.COLUMNAR_DELTA)
                .setCrc32Checksum(ChecksumUtil.computeCrc32(wirePayload))
                .setPayload(ByteString.copyFrom(wirePayload))
                .build();

        TelemetryBatchRepository batchRepository = mock(TelemetryBatchRepository.class);
        BatchIdempotencyService idempotency = mock(BatchIdempotencyService.class);
        TelemetryPersistenceService persistence = mock(TelemetryPersistenceService.class);
        CloudMetricsTracker metrics = mock(CloudMetricsTracker.class);
        when(idempotency.tryAcquire(valid.getBatchId())).thenReturn(true);

        TelemetryIngestGrpcService service = new TelemetryIngestGrpcService(
                idempotency, persistence, metrics, new ChaosController());
        RecordingObserver observer = new RecordingObserver();
        service.ingestBatch(valid, observer);

        assertTrue(observer.response.getAccepted());
        verify(persistence).persist(valid, events);

        CompressedBatchRequest corrupted = valid.toBuilder()
                .setPayload(valid.getPayload().concat(ByteString.copyFromUtf8("x")))
                .build();
        RecordingObserver invalidObserver = new RecordingObserver();
        service.ingestBatch(corrupted, invalidObserver);
        assertFalse(invalidObserver.response.getAccepted());
        assertTrue(invalidObserver.response.getMessage().contains("CRC32"));
        verify(metrics).recordChecksumFailure();

        RecordingObserver duplicateObserver = new RecordingObserver();
        when(idempotency.tryAcquire(valid.getBatchId())).thenReturn(false);
        service.ingestBatch(valid, duplicateObserver);
        assertTrue(duplicateObserver.response.getDuplicate());
        verify(metrics).recordDuplicate();
    }

    private static final class RecordingObserver
            implements StreamObserver<BatchAckResponse> {
        private BatchAckResponse response;
        private Throwable error;

        @Override
        public void onNext(BatchAckResponse value) {
            response = value;
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
        }

        @Override
        public void onCompleted() {
        }
    }
}
