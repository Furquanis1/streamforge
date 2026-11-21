package com.streamforge.common.codec;

import com.streamforge.common.compression.CompressionFactory;
import com.streamforge.common.compression.CompressionStrategy;
import com.streamforge.proto.CompressionType;
import com.streamforge.proto.EncodingType;
import com.streamforge.proto.SensorEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnarDeltaCodecTest {
    @Test
    void reconstructsRealisticTelemetryBitExactly() {
        Random random = new Random(12345L);
        List<SensorEvent> events = new ArrayList<>();

        for (int i = 0; i < 500; i++) {
            double value;
            if (i < 100) {
                value = 25.0;
            } else if (i < 300) {
                value = 25.0 + (i - 99) * 0.05;
            } else if (i == 300) {
                value = -125.75;
            } else if (i == 301) {
                value = Double.NaN;
            } else if (i == 302) {
                value = Double.POSITIVE_INFINITY;
            } else if (i == 303) {
                value = Double.NEGATIVE_INFINITY;
            } else if (i == 304) {
                value = Double.MAX_VALUE;
            } else {
                value = -((i - 304) % 17) * 0.125;
            }

            events.add(SensorEvent.newBuilder()
                    .setNodeId(i % 4 == 0 ? "node-a" : "node-b")
                    .setSensorType(i % 3 == 0 ? "temperature" : "pressure")
                    .setTimestampMs(1_700_000_000_000L + i * 100L)
                    .setValue(value)
                    .setAnomaly(random.nextBoolean())
                    .build());
        }

        BatchCodec codec = BatchCodecFactory.get(EncodingType.COLUMNAR_DELTA);
        List<SensorEvent> decoded = codec.decode(codec.encode(events));
        assertBitExactEvents(events, decoded);
    }

    @Test
    void zstdColumnarReductionExceedsSixtyPercentAgainstPlainRows() {
        List<SensorEvent> events = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            events.add(SensorEvent.newBuilder()
                    .setNodeId("node-" + (i % 5))
                    .setSensorType("temperature")
                    .setTimestampMs(1_700_000_000_000L + i * 1000L)
                    .setValue(25.0 + (i % 50) * 0.01)
                    .setAnomaly(i % 29 == 0)
                    .build());
        }

        BatchCodec plainCodec = BatchCodecFactory.get(EncodingType.PLAIN_ROW);
        BatchCodec columnarCodec = BatchCodecFactory.get(EncodingType.COLUMNAR_DELTA);
        CompressionStrategy zstd = CompressionFactory.get(CompressionType.ZSTD);

        byte[] plainEncoded = plainCodec.encode(events);
        byte[] plainCompressed = zstd.compress(plainEncoded);
        byte[] columnarEncoded = columnarCodec.encode(events);
        byte[] columnarCompressed = zstd.compress(columnarEncoded);

        double reduction =
                100.0 * (plainCompressed.length - columnarCompressed.length)
                        / plainCompressed.length;
        assertTrue(reduction >= 60.0,
                "Expected at least 60% size reduction, actual=" + reduction
                        + "% (plain=" + plainCompressed.length
                        + ", columnar=" + columnarCompressed.length + ")");
    }

    private static void assertBitExactEvents(List<SensorEvent> expected,
                                              List<SensorEvent> actual) {
        if (expected.size() != actual.size()) {
            throw new AssertionError("Record count differs");
        }
        for (int i = 0; i < expected.size(); i++) {
            SensorEvent original = expected.get(i);
            SensorEvent reconstructed = actual.get(i);
            if (!original.getNodeId().equals(reconstructed.getNodeId())
                    || !original.getSensorType().equals(reconstructed.getSensorType())
                    || original.getTimestampMs() != reconstructed.getTimestampMs()
                    || Double.doubleToRawLongBits(original.getValue())
                    != Double.doubleToRawLongBits(reconstructed.getValue())
                    || original.getAnomaly() != reconstructed.getAnomaly()) {
                throw new AssertionError("Event at index " + i + " differs bit-exactly");
            }
        }
    }
}
