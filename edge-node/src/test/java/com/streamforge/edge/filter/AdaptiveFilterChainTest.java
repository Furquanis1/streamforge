package com.streamforge.edge.filter;

import com.streamforge.proto.SensorEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptiveFilterChainTest {
    @Test
    void sanityFilterDropsInvalidPhysicalValues() {
        AdaptiveFilterChain chain = new AdaptiveFilterChain(3.0, 0.15, 10_000);

        SensorEvent nan = event("temperature", 1_000L, Double.NaN, false);
        SensorEvent temperature = event("temperature", 1_100L, 201.0, false);
        SensorEvent vibration = event("vibration", 1_200L, -1.0, false);
        SensorEvent pressure = event("pressure", 1_300L, 10_001.0, false);

        assertNull(chain.evaluate(nan));
        assertNull(chain.evaluate(temperature));
        assertNull(chain.evaluate(vibration));
        assertNull(chain.evaluate(pressure));
        assertTrue(chain.getDroppedSanity() >= 4);
    }

    @Test
    void deadbandSuppressesSteadyValuesAndHeartbeatBypassesAfterLimit() {
        AdaptiveFilterChain chain = new AdaptiveFilterChain(100.0, 0.15, 100);

        assertNotNull(chain.evaluate(event("temperature", 1_000L, 25.0, false)));
        assertNull(chain.evaluate(event("temperature", 1_050L, 25.05, false)));
        assertNotNull(chain.evaluate(event("temperature", 1_100L, 25.05, false)));
    }

    @Test
    void zscoreAnomalyAlwaysOverridesDeadband() {
        AdaptiveFilterChain chain = new AdaptiveFilterChain(3.0, 0.15, 10_000);

        for (int i = 0; i < 20; i++) {
            SensorEvent baseline = event(
                    "temperature",
                    1_000L + i * 100L,
                    25.0 + (i % 5) * 0.1,
                    false);
            chain.evaluate(baseline);
        }

        SensorEvent spike = event("temperature", 3_000L, 125.0, false);
        SensorEvent forwarded = chain.evaluate(spike);

        assertNotNull(forwarded);
        assertTrue(forwarded.getAnomaly());
        assertTrue(chain.getAnomaliesDetected() >= 1);
    }

    private static SensorEvent event(String sensorType, long timestamp,
                                     double value, boolean anomaly) {
        return SensorEvent.newBuilder()
                .setNodeId("test-node")
                .setSensorType(sensorType)
                .setTimestampMs(timestamp)
                .setValue(value)
                .setAnomaly(anomaly)
                .build();
    }
}
