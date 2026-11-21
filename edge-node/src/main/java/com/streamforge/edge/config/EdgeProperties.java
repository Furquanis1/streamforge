package com.streamforge.edge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "streamforge.edge")
public class EdgeProperties {
    private String brokerUrl = "tcp://localhost:1883";
    private String clientIdPrefix = "streamforge-edge-";
    private String topicPrefix = "telemetry/raw";
    private int nodeCount = 10;
    private int emitIntervalMs = 500;
    private int ringBufferCapacity = 5000;
    private double deadbandEpsilon = 0.15;
    private double zscoreThreshold = 3.0;
    private long maxHeartbeatMs = 10000;

    public String getBrokerUrl() { return brokerUrl; }
    public void setBrokerUrl(String brokerUrl) { this.brokerUrl = brokerUrl; }
    public String getClientIdPrefix() { return clientIdPrefix; }
    public void setClientIdPrefix(String clientIdPrefix) { this.clientIdPrefix = clientIdPrefix; }
    public String getTopicPrefix() { return topicPrefix; }
    public void setTopicPrefix(String topicPrefix) { this.topicPrefix = topicPrefix; }
    public int getNodeCount() { return nodeCount; }
    public void setNodeCount(int nodeCount) { this.nodeCount = nodeCount; }
    public int getEmitIntervalMs() { return emitIntervalMs; }
    public void setEmitIntervalMs(int emitIntervalMs) { this.emitIntervalMs = emitIntervalMs; }
    public int getRingBufferCapacity() { return ringBufferCapacity; }
    public void setRingBufferCapacity(int ringBufferCapacity) { this.ringBufferCapacity = ringBufferCapacity; }
    public double getDeadbandEpsilon() { return deadbandEpsilon; }
    public void setDeadbandEpsilon(double deadbandEpsilon) { this.deadbandEpsilon = deadbandEpsilon; }
    public double getZscoreThreshold() { return zscoreThreshold; }
    public void setZscoreThreshold(double zscoreThreshold) { this.zscoreThreshold = zscoreThreshold; }
    public long getMaxHeartbeatMs() { return maxHeartbeatMs; }
    public void setMaxHeartbeatMs(long maxHeartbeatMs) { this.maxHeartbeatMs = maxHeartbeatMs; }
}
