package com.streamforge.fog.mqtt;

import com.streamforge.fog.config.FogProperties;
import com.streamforge.proto.SensorEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;

@Component
public class FogMqttConsumer {
    public static final int QUEUE_CAPACITY = 20_000;
    private final FogProperties properties;
    private final LinkedBlockingQueue<SensorEvent> ingestionQueue =
            new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private volatile MqttClient client;

    public FogMqttConsumer(FogProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        try {
            client = new MqttClient(
                    properties.getBrokerUrl(),
                    properties.getClientId(),
                    new MemoryPersistence());
            client.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    subscribe();
                }

                @Override
                public void connectionLost(Throwable cause) {
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    try {
                        SensorEvent event = SensorEvent.parseFrom(message.getPayload());
                        if (!ingestionQueue.offer(event)) {
                            SensorEvent overflow = ingestionQueue.poll();
                            ingestionQueue.offer(overflow == null ? event : event);
                        }
                    } catch (Exception ignored) {
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(false);
            client.connect(options);
            subscribe();
        } catch (MqttException ignored) {
        }
    }

    private void subscribe() {
        if (client == null || !client.isConnected()) {
            return;
        }
        try {
            client.subscribe(properties.getSubscribeTopic(), 1);
        } catch (MqttException ignored) {
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
