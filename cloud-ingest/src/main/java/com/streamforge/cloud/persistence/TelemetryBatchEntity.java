package com.streamforge.cloud.persistence;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Column;

import java.time.Instant;

@Entity
@Table(name = "telemetry_batches")
public class TelemetryBatchEntity {
    @Id
    @Column(name = "batch_id", length = 36)
    private String batchId;
    private String gatewayId;
    private String encodingType;
    private String compressionType;
    private int eventCount;
    private int uncompressedBytes;
    private int compressedBytes;
    private long crc32Checksum;
    private long batchTimestampMs;
    private Instant ingestedAt;

    public TelemetryBatchEntity() {
    }

    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }
    public String getGatewayId() { return gatewayId; }
    public void setGatewayId(String gatewayId) { this.gatewayId = gatewayId; }
    public String getEncodingType() { return encodingType; }
    public void setEncodingType(String encodingType) { this.encodingType = encodingType; }
    public String getCompressionType() { return compressionType; }
    public void setCompressionType(String compressionType) { this.compressionType = compressionType; }
    public int getEventCount() { return eventCount; }
    public void setEventCount(int eventCount) { this.eventCount = eventCount; }
    public int getUncompressedBytes() { return uncompressedBytes; }
    public void setUncompressedBytes(int uncompressedBytes) { this.uncompressedBytes = uncompressedBytes; }
    public int getCompressedBytes() { return compressedBytes; }
    public void setCompressedBytes(int compressedBytes) { this.compressedBytes = compressedBytes; }
    public long getCrc32Checksum() { return crc32Checksum; }
    public void setCrc32Checksum(long crc32Checksum) { this.crc32Checksum = crc32Checksum; }
    public long getBatchTimestampMs() { return batchTimestampMs; }
    public void setBatchTimestampMs(long batchTimestampMs) { this.batchTimestampMs = batchTimestampMs; }
    public Instant getIngestedAt() { return ingestedAt; }
    public void setIngestedAt(Instant ingestedAt) { this.ingestedAt = ingestedAt; }
}
