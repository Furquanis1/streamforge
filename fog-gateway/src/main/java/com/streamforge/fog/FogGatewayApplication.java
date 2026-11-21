package com.streamforge.fog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class FogGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(FogGatewayApplication.class, args);
    }
}
