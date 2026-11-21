package com.streamforge.edge.mqtt;

import com.streamforge.edge.config.EdgeProperties;
import com.streamforge.proto.SensorEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class EdgeMqttPublisher {
    private static final Logger log = LoggerFactory.getLogger(EdgeMqttPublisher.class);
    private final EdgeProperties properties;
    private final BoundedEventRingBuffer ringBuffer;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private MqttAsyncClient client;
    private ExecutorService connectionExecutor;

    public EdgeMqttPublisher(EdgeProperties properties) {
        this.properties = properties;
        this.ringBuffer = new BoundedEventRingBuffer(properties.getRingBufferCapacity());
    }

    @PostConstruct
    public void start() {
        connectionExecutor = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("edge-mqtt-", 0).factory());
        connectionExecutor.submit(this::connect);
    }

    private void connect() {
        try {
            String clientId = properties.getClientIdPrefix()
                    + UUID.randomUUID().toString().substring(0, 8);
            client = new MqttAsyncClient(
                    properties.getBrokerUrl(),
                    clientId,
                    new MemoryPersistence());
            client.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    connected.set(true);
                    log.info("Connected to MQTT broker {}{}", serverURI,
                            reconnect ? " (reconnected)" : "");
                    drainBufferedEvents();
                }

                @Override
                public void connectionLost(Throwable cause) {
                    connected.set(false);
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(5);
            options.setKeepAliveInterval(15);
            client.connect(options).waitForCompletion(5000);
            connected.set(true);
            drainBufferedEvents();
        } catch (MqttException e) {
            connected.set(false);
            log.warn("MQTT connection pending for {}: {}", properties.getBrokerUrl(), e.getMessage());
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void mqttWatchdog() {
        if (client == null) {
            connectionExecutor.submit(this::connect);
            return;
        }
        if (client.isConnected()) {
            connected.set(true);
            drainBufferedEvents();
            return;
        }

        connected.set(false);
        try {
            client.reconnect();
            log.info("MQTT reconnect initiated for broker {}", properties.getBrokerUrl());
        } catch (MqttException e) {
            log.warn("MQTT reconnect failed for {}: {}", properties.getBrokerUrl(), e.getMessage());
        }
    }

    public boolean publish(SensorEvent event) {
        if (!connected.get() || client == null || !client.isConnected()) {
            ringBuffer.offer(event);
            return false;
        }

        try {
            String topic = properties.getTopicPrefix() + "/"
                    + event.getNodeId() + "/" + event.getSensorType();
            MqttMessage message = new MqttMessage(event.toByteArray());
            message.setQos(1);
            message.setRetained(false);
            client.publish(topic, message);
            return true;
        } catch (MqttException e) {
            connected.set(false);
            ringBuffer.offer(event);
            return false;
        }
    }

    private void drainBufferedEvents() {
        while (connected.get() && !ringBuffer.isEmpty()) {
            SensorEvent event = ringBuffer.poll();
            if (event == null) {
                break;
            }
            if (!publishWithoutBuffering(event)) {
                ringBuffer.offer(event);
                break;
            }
        }
    }

    private boolean publishWithoutBuffering(SensorEvent event) {
        try {
            String topic = properties.getTopicPrefix() + "/"
                    + event.getNodeId() + "/" + event.getSensorType();
            MqttMessage message = new MqttMessage(event.toByteArray());
            message.setQos(1);
            client.publish(topic, message);
            return true;
        } catch (MqttException e) {
            connected.set(false);
            return false;
        }
    }

    public boolean isConnected() {
        return connected.get();
    }

    public int getRingBufferSize() {
        return ringBuffer.size();
    }

    public long getRingBufferEvictedCount() {
        return ringBuffer.getEvictedCount();
    }

    @PreDestroy
    public void close() {
        connected.set(false);
        if (client != null) {
            try {
                client.disconnect();
                client.close();
            } catch (MqttException ignored) {
            }
        }
        if (connectionExecutor != null) {
            connectionExecutor.shutdownNow();
        }
    }
}
