package com.streamforge.cloud.persistence;

import com.streamforge.proto.CompressedBatchRequest;
import com.streamforge.proto.SensorEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class TelemetryPersistenceService {
    private final TelemetryBatchRepository batchRepository;
    private final SensorReadingRepository readingRepository;

    public TelemetryPersistenceService(TelemetryBatchRepository batchRepository,
                                       SensorReadingRepository readingRepository) {
        this.batchRepository = batchRepository;
        this.readingRepository = readingRepository;
    }

    @Transactional
    public void persist(CompressedBatchRequest request,
                        List<SensorEvent> events) {
        TelemetryBatchEntity batch = new TelemetryBatchEntity();
        batch.setBatchId(request.getBatchId());
        batch.setGatewayId(request.getFogNodeId());
        batch.setEncodingType(request.getEncoding().name());
        batch.setCompressionType(request.getCompression().name());
        batch.setEventCount(request.getRecordCount());
        batch.setUncompressedBytes(request.getRawBytesSize());
        batch.setCompressedBytes(request.getCompressedBytesSize());
        batch.setCrc32Checksum(request.getCrc32Checksum());
        batch.setBatchTimestampMs(request.getCreatedAtMs());
        batch.setIngestedAt(Instant.now());
        batchRepository.save(batch);

        List<SensorReadingEntity> readings = new ArrayList<>(events.size());
        for (SensorEvent event : events) {
            SensorReadingEntity reading = new SensorReadingEntity();
            reading.setBatchId(request.getBatchId());
            reading.setNodeId(event.getNodeId());
            reading.setSensorType(event.getSensorType());
            reading.setReadingValue(event.getValue());
            reading.setUnit(unitForSensor(event.getSensorType()));
            reading.setAnomaly(event.getAnomaly());
            reading.setTimestampMs(event.getTimestampMs());
            readings.add(reading);
        }
        readingRepository.saveAll(readings);
    }

    private static String unitForSensor(String sensorType) {
        return switch (sensorType) {
            case "temperature" -> "C";
            case "vibration" -> "mm/s";
            case "pressure" -> "hPa";
            default -> "";
        };
    }
}
