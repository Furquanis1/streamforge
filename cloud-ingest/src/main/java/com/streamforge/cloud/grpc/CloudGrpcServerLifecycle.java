package com.streamforge.cloud.grpc;

import com.streamforge.cloud.config.CloudProperties;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class CloudGrpcServerLifecycle implements SmartLifecycle {
    private final CloudProperties properties;
    private final TelemetryIngestGrpcService service;
    private volatile Server server;
    private volatile boolean running;

    public CloudGrpcServerLifecycle(CloudProperties properties,
                                    TelemetryIngestGrpcService service) {
        this.properties = properties;
        this.service = service;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        try {
            server = ServerBuilder.forPort(properties.getGrpcPort())
                    .addService(service)
                    .build()
                    .start();
            running = true;
        } catch (IOException e) {
            throw new IllegalStateException("Could not start gRPC server", e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdownNow();
            server = null;
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
