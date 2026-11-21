package com.streamforge.edge.mqtt;

import com.streamforge.proto.SensorEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BoundedEventRingBufferTest {
    @Test
    void drainsInFifoOrder() {
        BoundedEventRingBuffer buffer = new BoundedEventRingBuffer(3);
        buffer.offer(event("node-a"));
        buffer.offer(event("node-b"));
        buffer.offer(event("node-c"));

        assertEquals("node-a", buffer.poll().getNodeId());
        assertEquals("node-b", buffer.poll().getNodeId());
        assertEquals("node-c", buffer.poll().getNodeId());
        assertNull(buffer.poll());
    }

    @Test
    void evictsOldestWhenCapacityIsExceeded() {
        BoundedEventRingBuffer buffer = new BoundedEventRingBuffer(2);
        buffer.offer(event("node-a"));
        buffer.offer(event("node-b"));
        buffer.offer(event("node-c"));

        assertEquals(1, buffer.getEvictedCount());
        assertEquals("node-b", buffer.poll().getNodeId());
        assertEquals("node-c", buffer.poll().getNodeId());
        assertEquals(0, buffer.size());
    }

    private static SensorEvent event(String nodeId) {
        return SensorEvent.newBuilder()
                .setNodeId(nodeId)
                .setSensorType("temperature")
                .setTimestampMs(1_700_000_000_000L)
                .setValue(25.0)
                .setAnomaly(false)
                .build();
    }
}
