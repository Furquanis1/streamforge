package com.streamforge.cloud.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TelemetryBatchRepository extends JpaRepository<TelemetryBatchEntity, String> {
    List<TelemetryBatchEntity> findTop25ByOrderByIngestedAtDesc();

    @Query("select coalesce(sum(b.eventCount), 0) from TelemetryBatchEntity b")
    long sumEventCount();

    @Query("select coalesce(sum(b.uncompressedBytes), 0) from TelemetryBatchEntity b")
    long sumUncompressedBytes();

    @Query("select coalesce(sum(b.compressedBytes), 0) from TelemetryBatchEntity b")
    long sumCompressedBytes();

    @Query("select b from TelemetryBatchEntity b where b.batchTimestampMs >= :sinceMs order by b.batchTimestampMs desc")
    List<TelemetryBatchEntity> findRecentAfter(@Param("sinceMs") long sinceMs);
}
