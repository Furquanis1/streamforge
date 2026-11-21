package com.streamforge.fog.batch;

import com.streamforge.fog.adaptive.AdaptiveCompressionSelector;
import com.streamforge.fog.config.FogProperties;
import com.streamforge.fog.grpc.CloudGrpcForwarder;
import com.streamforge.fog.mqtt.FogMqttConsumer;
import com.streamforge.fog.wal.DiskWriteAheadLog;
import com.streamforge.proto.CompressedBatchRequest;
import com.streamforge.proto.SensorEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Configuration
public class FogBatchBeans {
    @Bean
    public DiskWriteAheadLog diskWriteAheadLog(FogProperties properties) {
        return new DiskWriteAheadLog(
                properties.getWalDirectory(),
                properties.getWalMaxBytes());
    }

    @Bean
    public AdaptiveCompressionSelector adaptiveCompressionSelector(
            FogProperties properties) {
        return new AdaptiveCompressionSelector(
                properties.getHighThroughputEventsPerSec(),
                properties.getMinCompressionGainRatio());
    }

    @Bean
    public MicroBatchAccumulator microBatchAccumulator(
            FogProperties properties,
            FogMqttConsumer consumer,
            AdaptiveCompressionSelector selector,
            CloudGrpcForwarder forwarder) {
        MicroBatchAccumulator accumulator = new MicroBatchAccumulator(
                consumer.getIngestionQueue(),
                properties.getBatchMaxSize(),
                properties.getBatchFlushIntervalMs());
        accumulator.setFlushHandler(events ->
                forwarder.forwardOrSpill(selector.buildTelemetryBatch(
                        events,
                        properties.getClientId(),
                        0)));
        accumulator.start();
        return accumulator;
    }
}
