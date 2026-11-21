package com.streamforge.cloud.persistence;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface SensorReadingRepository extends JpaRepository<SensorReadingEntity, Long> {
    @Query("select r from SensorReadingEntity r order by r.timestampMs desc")
    List<SensorReadingEntity> findRecent(Pageable pageable);

    @Query("select r from SensorReadingEntity r where r.anomaly = true order by r.timestampMs desc")
    List<SensorReadingEntity> findRecentAnomalies(Pageable pageable);

    @Query("select r from SensorReadingEntity r where r.nodeId = :nodeId and r.sensorType = :sensorType order by r.timestampMs desc")
    List<SensorReadingEntity> findLatestForNodeSensor(
            String nodeId, String sensorType, Pageable pageable);

    @Query("select coalesce(avg(r.readingValue), 0) from SensorReadingEntity r")
    double aggregateAverageReading();
}
