package com.streamforge.edge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class EdgeNodeApplication {
    public static void main(String[] args) {
        SpringApplication.run(EdgeNodeApplication.class, args);
    }
}
