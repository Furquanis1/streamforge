package com.streamforge.fog.batch;

import com.streamforge.proto.SensorEvent;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroBatchAccumulatorTest {
    @Test
    void sizeTriggerFlushesAtMaximumSize() {
        MicroBatchAccumulator accumulator =
                new MicroBatchAccumulator(new LinkedBlockingQueue<>(), 3, 10_000);
        List<SensorEvent>[] captured = new List[]{List.of()};
        accumulator.setFlushHandler(events -> captured[0] = events);
        accumulator.offer(event(false));
        accumulator.offer(event(false));
        accumulator.offer(event(false));
        assertEquals(0, accumulator.size());
        assertEquals(3, captured[0].size());
    }

    @Test
    void timeTriggerFlushesAfterWindow() throws Exception {
        MicroBatchAccumulator accumulator =
                new MicroBatchAccumulator(new LinkedBlockingQueue<>(), 250, 20);
        accumulator.offer(event(false));
        Thread.sleep(30);
        assertEquals(1, accumulator.pollReady().size());
    }

    @Test
    void anomalyEventFlushesFastPath() {
        MicroBatchAccumulator accumulator =
                new MicroBatchAccumulator(new LinkedBlockingQueue<>(), 250, 10_000);
        List<SensorEvent>[] captured = new List[]{List.of()};
        accumulator.setFlushHandler(events -> captured[0] = events);
        accumulator.offer(event(false));
        accumulator.offer(event(true));
        assertTrue(captured[0].size() >= 2);
    }

    private static SensorEvent event(boolean anomaly) {
        return SensorEvent.newBuilder()
                .setNodeId("node-a")
                .setSensorType("temperature")
                .setTimestampMs(System.currentTimeMillis())
                .setValue(25.0)
                .setAnomaly(anomaly)
                .build();
    }
}
