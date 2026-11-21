package com.streamforge.cloud.chaos;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ChaosController {
    private final AtomicBoolean failMode = new AtomicBoolean();
    private final AtomicInteger latencyMs = new AtomicInteger();

    public boolean isFailMode() {
        return failMode.get();
    }

    public void setFailMode(boolean failMode) {
        this.failMode.set(failMode);
    }

    public int getLatencyMs() {
        return latencyMs.get();
    }

    public void setLatencyMs(int latencyMs) {
        if (latencyMs < 0) {
            throw new IllegalArgumentException("latencyMs must not be negative");
        }
        this.latencyMs.set(latencyMs);
    }
}
