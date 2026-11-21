package com.streamforge.common.compression;

import com.streamforge.proto.CompressionType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class CompressionStrategyTest {
    @Test
    void roundTripsEmptySmallAndTenThousandRecordPayloadsForAllStrategies() {
        Random random = new Random(42L);
        byte[] small = "StreamForge telemetry".getBytes(StandardCharsets.UTF_8);
        byte[] tenThousandRecords = new byte[10_000 * 32];
        for (int i = 0; i < 10_000; i++) {
            random.nextBytes(tenThousandRecords);
        }

        for (CompressionType type : CompressionType.values()) {
            if (type == CompressionType.UNRECOGNIZED) {
                continue;
            }
            CompressionStrategy strategy = CompressionFactory.get(type);
            assertPayloadRoundTrip(strategy, new byte[0]);
            assertPayloadRoundTrip(strategy, small);
            assertPayloadRoundTrip(strategy, tenThousandRecords);
        }
    }

    private static void assertPayloadRoundTrip(CompressionStrategy strategy, byte[] payload) {
        byte[] compressed = strategy.compress(payload);
        byte[] restored = strategy.decompress(compressed, payload.length);
        assertArrayEquals(payload, restored, strategy.getType() + " round trip failed");
    }
}
