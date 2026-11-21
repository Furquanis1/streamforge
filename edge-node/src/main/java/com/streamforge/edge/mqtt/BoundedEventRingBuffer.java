package com.streamforge.edge.mqtt;

import com.streamforge.proto.SensorEvent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public final class BoundedEventRingBuffer {
    private final int capacity;
    private final Deque<SensorEvent> events;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong evictedCount = new AtomicLong();

    public BoundedEventRingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Ring buffer capacity must be positive");
        }
        this.capacity = capacity;
        this.events = new ArrayDeque<>(capacity);
    }

    public boolean offer(SensorEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Event must not be null");
        }
        lock.lock();
        try {
            if (events.size() == capacity) {
                events.removeFirst();
                evictedCount.incrementAndGet();
            }
            events.addLast(event);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public SensorEvent poll() {
        lock.lock();
        try {
            return events.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return events.size();
        } finally {
            lock.unlock();
        }
    }

    public long getEvictedCount() {
        return evictedCount.get();
    }

    public boolean isEmpty() {
        return size() == 0;
    }
}
