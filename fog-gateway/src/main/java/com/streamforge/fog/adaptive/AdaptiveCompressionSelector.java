package com.streamforge.fog.adaptive;

import com.streamforge.common.codec.BatchCodec;
import com.streamforge.common.codec.BatchCodecFactory;
import com.streamforge.common.compression.CompressionFactory;
import com.streamforge.common.compression.CompressionStrategy;
import com.streamforge.common.integrity.ChecksumUtil;
import com.streamforge.proto.CompressedBatchRequest;
import com.streamforge.proto.CompressionType;
import com.streamforge.proto.EncodingType;
import com.streamforge.proto.SensorEvent;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class AdaptiveCompressionSelector {
    private final int highThroughputEventsPerSec;
    private final double minCompressionGainRatio;
    private volatile String overrideMode = "AUTO";
    private volatile double currentIngressRate;

    public record Selection(EncodingType encoding, CompressionType compression,
                            byte[] encodedPayload, byte[] wirePayload,
                            long crc32Checksum) {
    }

    public AdaptiveCompressionSelector(int highThroughputEventsPerSec,
                                       double minCompressionGainRatio) {
        this.highThroughputEventsPerSec = highThroughputEventsPerSec;
        this.minCompressionGainRatio = minCompressionGainRatio;
    }

    public void setIngressRate(double ingressRate) {
        this.currentIngressRate = ingressRate;
    }

    public double getIngressRate() {
        return currentIngressRate;
    }

    public void setOverrideMode(String overrideMode) {
        if (overrideMode == null) {
            throw new IllegalArgumentException("overrideMode must not be null");
        }
        this.overrideMode = switch (overrideMode.toUpperCase()) {
            case "AUTO", "RAW_NONE", "LZ4", "ZSTD", "GZIP" -> overrideMode.toUpperCase();
            default -> throw new IllegalArgumentException(
                    "Unsupported compression override: " + overrideMode);
        };
    }

    public String getOverrideMode() {
        return overrideMode;
    }

    public Selection select(List<SensorEvent> events) {
        if (events == null) {
            throw new IllegalArgumentException("events must not be null");
        }
        if (events.size() < 10) {
            BatchCodec plain = BatchCodecFactory.get(EncodingType.PLAIN_ROW);
            byte[] encoded = plain.encode(events);
            return new Selection(EncodingType.PLAIN_ROW, CompressionType.RAW_NONE,
                    encoded, encoded, ChecksumUtil.computeCrc32(encoded));
        }

        EncodingType encoding = EncodingType.COLUMNAR_DELTA;
        CompressionType compression;
        if ("AUTO".equals(overrideMode)) {
            compression = currentIngressRate >= highThroughputEventsPerSec
                    ? CompressionType.LZ4
                    : (hasHighCompressibility(events)
                    ? CompressionType.ZSTD
                    : CompressionType.LZ4);
        } else {
            encoding = "RAW_NONE".equals(overrideMode)
                    ? EncodingType.PLAIN_ROW
                    : EncodingType.COLUMNAR_DELTA;
            compression = switch (overrideMode) {
                case "RAW_NONE" -> CompressionType.RAW_NONE;
                case "LZ4" -> CompressionType.LZ4;
                case "ZSTD" -> CompressionType.ZSTD;
                case "GZIP" -> CompressionType.GZIP;
                default -> CompressionType.ZSTD;
            };
        }

        BatchCodec codec = BatchCodecFactory.get(encoding);
        byte[] encoded = codec.encode(events);
        CompressionStrategy strategy = CompressionFactory.get(compression);
        byte[] compressed = strategy.compress(encoded);

        if (compressed.length * minCompressionGainRatio >= encoded.length) {
            if (compression != CompressionType.RAW_NONE) {
                encoding = EncodingType.PLAIN_ROW;
                compression = CompressionType.RAW_NONE;
                BatchCodec plain = BatchCodecFactory.get(encoding);
                encoded = plain.encode(events);
                compressed = encoded;
            }
        }

        return new Selection(encoding, compression, encoded, compressed,
                ChecksumUtil.computeCrc32(compressed));
    }

    public CompressedBatchRequest buildTelemetryBatch(List<SensorEvent> events,
                                                       String fogNodeId,
                                                       int edgeDroppedCount) {
        Selection selection = select(events);
        long now = System.currentTimeMillis();
        return CompressedBatchRequest.newBuilder()
                .setBatchId(UUID.randomUUID().toString())
                .setFogNodeId(fogNodeId)
                .setCreatedAtMs(now)
                .setRecordCount(events.size())
                .setRawBytesSize(selection.encodedPayload().length)
                .setCompressedBytesSize(selection.wirePayload().length)
                .setEdgeDroppedCount(edgeDroppedCount)
                .setCompression(selection.compression())
                .setEncoding(selection.encoding())
                .setCrc32Checksum(selection.crc32Checksum())
                .setPayload(ByteString.copyFrom(selection.wirePayload()))
                .build();
    }

    private static boolean hasHighCompressibility(List<SensorEvent> events) {
        if (events.isEmpty()) {
            return false;
        }
        double mean = 0.0;
        double m2 = 0.0;
        long count = 0;
        for (SensorEvent event : events) {
            count++;
            double delta = event.getValue() - mean;
            mean += delta / count;
            m2 += delta * (event.getValue() - mean);
        }
        double variance = count > 1 ? m2 / (count - 1) : 0.0;
        return variance < 100.0;
    }
}
