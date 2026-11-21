package com.streamforge.fog.mqtt;

import com.streamforge.fog.config.FogProperties;
import com.streamforge.proto.SensorEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class FogMqttConsumer {
    private static final Logger log = LoggerFactory.getLogger(FogMqttConsumer.class);
    public static final int QUEUE_CAPACITY = 20_000;
    private final FogProperties properties;
    private final LinkedBlockingQueue<SensorEvent> ingestionQueue =
            new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private volatile MqttAsyncClient client;
    private ScheduledExecutorService connectionExecutor;

    public FogMqttConsumer(FogProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        connectionExecutor = Executors.newSingleThreadScheduledExecutor(
                runnable -> Thread.ofPlatform().daemon(true).name("fog-mqtt").unstarted(runnable));
        connectionExecutor.scheduleWithFixedDelay(this::connect, 0, 5, TimeUnit.SECONDS);
    }

    private void connect() {
        if (client != null && client.isConnected()) {
            return;
        }
        try {
            String suffix = UUID.randomUUID().toString().substring(0, 8);
            MqttAsyncClient candidate = new MqttAsyncClient(
                    properties.getBrokerUrl(),
                    properties.getClientId() + "-" + suffix,
                    new MemoryPersistence());
            candidate.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    subscribe(candidate);
                    log.info("Connected to MQTT broker {}{}", serverURI,
                            reconnect ? " (reconnected)" : "");
                }

                @Override
                public void connectionLost(Throwable cause) {
                    log.warn("MQTT connection lost: {}", cause == null ? "unknown" : cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    try {
                        SensorEvent event = SensorEvent.parseFrom(message.getPayload());
                        if (!ingestionQueue.offer(event)) {
                            SensorEvent overflow = ingestionQueue.poll();
                            ingestionQueue.offer(overflow == null ? event : overflow);
                        }
                    } catch (Exception e) {
                        log.error("Failed to deserialize SensorEvent from MQTT topic {} ({} bytes): {}",
                                topic, message.getPayload().length, e.getMessage());
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            options.setKeepAliveInterval(20);
            candidate.connect(options).waitForCompletion(5000);
            client = candidate;
            subscribe(candidate);
        } catch (MqttSecurityException e) {
            log.error("MQTT authentication rejected by {}: {}", properties.getBrokerUrl(), e.getMessage());
        } catch (MqttException e) {
            log.warn("MQTT connection pending for {}: {}", properties.getBrokerUrl(), e.getMessage());
        }
    }

    private void subscribe(MqttAsyncClient mqttClient) {
        if (mqttClient == null || !mqttClient.isConnected()) {
            return;
        }
        try {
            mqttClient.subscribe(properties.getSubscribeTopic(), 1);
            log.info("Subscribed to {} at broker {}", properties.getSubscribeTopic(), properties.getBrokerUrl());
        } catch (MqttException e) {
            log.warn("Subscription failed for {}: {}", properties.getSubscribeTopic(), e.getMessage());
        }
    }

    public boolean offer(SensorEvent event) {
        return ingestionQueue.offer(event);
    }

    public SensorEvent poll() {
        return ingestionQueue.poll();
    }

    public LinkedBlockingQueue<SensorEvent> getIngestionQueue() {
        return ingestionQueue;
    }

    public int getQueueSize() {
        return ingestionQueue.size();
    }

    @PreDestroy
    public void close() {
        if (connectionExecutor != null) {
            connectionExecutor.shutdownNow();
        }
        if (client == null) {
            return;
        }
        try {
            client.disconnect();
            client.close();
        } catch (MqttException ignored) {
        }
    }
}
