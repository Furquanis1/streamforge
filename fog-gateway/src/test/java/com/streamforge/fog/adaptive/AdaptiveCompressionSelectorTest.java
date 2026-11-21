package com.streamforge.fog.adaptive;

import com.streamforge.proto.CompressionType;
import com.streamforge.proto.EncodingType;
import com.streamforge.proto.SensorEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdaptiveCompressionSelectorTest {
    @Test
    void switchesToLz4AtHighIngressAndZstdAtNormalIngress() {
        List<SensorEvent> batch = constantBatch(500);
        AdaptiveCompressionSelector selector =
                new AdaptiveCompressionSelector(500, 1.15);

        selector.setIngressRate(600);
        AdaptiveCompressionSelector.Selection high = selector.select(batch);
        assertEquals(CompressionType.LZ4, high.compression());

        selector.setIngressRate(100);
        AdaptiveCompressionSelector.Selection normal = selector.select(batch);
        assertEquals(CompressionType.ZSTD, normal.compression());
    }

    @Test
    void fallsBackToPlainRawForSmallOrPoorGainBatches() {
        AdaptiveCompressionSelector selector =
                new AdaptiveCompressionSelector(500, 1.15);
        AdaptiveCompressionSelector.Selection small =
                selector.select(constantBatch(9));
        assertEquals(EncodingType.PLAIN_ROW, small.encoding());
        assertEquals(CompressionType.RAW_NONE, small.compression());
    }

    private static List<SensorEvent> constantBatch(int count) {
        List<SensorEvent> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            events.add(SensorEvent.newBuilder()
                    .setNodeId("node-a")
                    .setSensorType("temperature")
                    .setTimestampMs(1_700_000_000_000L + i * 100L)
                    .setValue(25.0)
                    .setAnomaly(false)
                    .build());
        }
        return events;
    }
}
