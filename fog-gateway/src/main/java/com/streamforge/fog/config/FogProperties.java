package com.streamforge.fog.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "streamforge.fog")
public class FogProperties {
    private String brokerUrl = "tcp://localhost:1883";
    private String clientId = "streamforge-fog-gateway";
    private String subscribeTopic = "telemetry/raw/#";
    private String cloudGrpcHost = "localhost";
    private int cloudGrpcPort = 9090;
    private int batchMaxSize = 250;
    private long batchFlushIntervalMs = 2000;
    private String walDirectory = "./wal";
    private long walMaxBytes = 52428800L;
    private int highThroughputEventsPerSec = 500;
    private double minCompressionGainRatio = 1.15;

    public String getBrokerUrl() { return brokerUrl; }
    public void setBrokerUrl(String brokerUrl) { this.brokerUrl = brokerUrl; }
    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }
    public String getSubscribeTopic() { return subscribeTopic; }
    public void setSubscribeTopic(String subscribeTopic) { this.subscribeTopic = subscribeTopic; }
    public String getCloudGrpcHost() { return cloudGrpcHost; }
    public void setCloudGrpcHost(String cloudGrpcHost) { this.cloudGrpcHost = cloudGrpcHost; }
    public int getCloudGrpcPort() { return cloudGrpcPort; }
    public void setCloudGrpcPort(int cloudGrpcPort) { this.cloudGrpcPort = cloudGrpcPort; }
    public int getBatchMaxSize() { return batchMaxSize; }
    public void setBatchMaxSize(int batchMaxSize) { this.batchMaxSize = batchMaxSize; }
    public long getBatchFlushIntervalMs() { return batchFlushIntervalMs; }
    public void setBatchFlushIntervalMs(long batchFlushIntervalMs) { this.batchFlushIntervalMs = batchFlushIntervalMs; }
    public String getWalDirectory() { return walDirectory; }
    public void setWalDirectory(String walDirectory) { this.walDirectory = walDirectory; }
    public long getWalMaxBytes() { return walMaxBytes; }
    public void setWalMaxBytes(long walMaxBytes) { this.walMaxBytes = walMaxBytes; }
    public int getHighThroughputEventsPerSec() { return highThroughputEventsPerSec; }
    public void setHighThroughputEventsPerSec(int highThroughputEventsPerSec) { this.highThroughputEventsPerSec = highThroughputEventsPerSec; }
    public double getMinCompressionGainRatio() { return minCompressionGainRatio; }
    public void setMinCompressionGainRatio(double minCompressionGainRatio) { this.minCompressionGainRatio = minCompressionGainRatio; }
}
