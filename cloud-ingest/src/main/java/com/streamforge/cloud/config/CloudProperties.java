package com.streamforge.cloud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "streamforge.cloud")
public class CloudProperties {
    private int grpcPort = 9090;
    private long idempotencyTtlSeconds = 86400;
    private long sseHeartbeatMs = 2000;

    public int getGrpcPort() { return grpcPort; }
    public void setGrpcPort(int grpcPort) { this.grpcPort = grpcPort; }
    public long getIdempotencyTtlSeconds() { return idempotencyTtlSeconds; }
    public void setIdempotencyTtlSeconds(long idempotencyTtlSeconds) {
        this.idempotencyTtlSeconds = idempotencyTtlSeconds;
    }
    public long getSseHeartbeatMs() { return sseHeartbeatMs; }
    public void setSseHeartbeatMs(long sseHeartbeatMs) { this.sseHeartbeatMs = sseHeartbeatMs; }
}
