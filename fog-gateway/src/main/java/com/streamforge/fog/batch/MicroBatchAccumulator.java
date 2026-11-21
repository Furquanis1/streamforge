package com.streamforge.fog.batch;

import com.streamforge.proto.SensorEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class MicroBatchAccumulator {
    private final LinkedBlockingQueue<SensorEvent> ingestionQueue;
    private final int batchMaxSize;
    private final long batchFlushIntervalMs;
    private final List<SensorEvent> batch = new ArrayList<>();
    private volatile long firstEventAtMs;
    private volatile Consumer<List<SensorEvent>> flushHandler = ignored -> { };
    private final AtomicReference<Thread> worker = new AtomicReference<>();

    public MicroBatchAccumulator(LinkedBlockingQueue<SensorEvent> ingestionQueue,
                                 int batchMaxSize,
                                 long batchFlushIntervalMs) {
        if (batchMaxSize <= 0) {
            throw new IllegalArgumentException("batchMaxSize must be positive");
        }
        this.ingestionQueue = ingestionQueue;
        this.batchMaxSize = batchMaxSize;
        this.batchFlushIntervalMs = batchFlushIntervalMs;
    }

    public MicroBatchAccumulator(int batchMaxSize, long batchFlushIntervalMs) {
        this(new LinkedBlockingQueue<>(), batchMaxSize, batchFlushIntervalMs);
    }

    public void setFlushHandler(Consumer<List<SensorEvent>> flushHandler) {
        this.flushHandler = flushHandler;
    }

    public synchronized boolean offer(SensorEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        if (batch.isEmpty()) {
            firstEventAtMs = System.currentTimeMillis();
        }
        batch.add(event);
        if (batch.size() >= batchMaxSize) {
            flushNow();
            return true;
        }
        if (event.getAnomaly()) {
            flushNow();
        }
        return true;
    }

    public synchronized List<SensorEvent> pollReady() {
        if (batch.isEmpty()) {
            return List.of();
        }
        long elapsed = System.currentTimeMillis() - firstEventAtMs;
        if (elapsed >= batchFlushIntervalMs) {
            return flushNow();
        }
        return List.of();
    }

    public synchronized List<SensorEvent> drainReady() {
        while (true) {
            SensorEvent event = ingestionQueue.poll();
            if (event == null) {
                break;
            }
            if (batch.isEmpty()) {
                firstEventAtMs = System.currentTimeMillis();
            }
            batch.add(event);
            if (batch.size() >= batchMaxSize) {
                return flushNow();
            }
            if (event.getAnomaly()) {
                return flushNow();
            }
        }
        if (!batch.isEmpty()
                && System.currentTimeMillis() - firstEventAtMs >= batchFlushIntervalMs) {
            return flushNow();
        }
        return List.of();
    }

    public synchronized List<SensorEvent> flushNow() {
        if (batch.isEmpty()) {
            return List.of();
        }
        List<SensorEvent> result = new ArrayList<>(batch);
        batch.clear();
        firstEventAtMs = 0;
        flushHandler.accept(result);
        return result;
    }

    public synchronized int size() {
        return batch.size();
    }

    public void start() {
        Thread thread = Thread.ofVirtual().name("fog-micro-batcher").start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    SensorEvent event = ingestionQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        offer(event);
                    } else {
                        pollReady();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        worker.set(thread);
    }

    public void stop() {
        Thread thread = worker.getAndSet(null);
        if (thread != null) {
            thread.interrupt();
        }
    }
}
