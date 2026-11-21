package com.streamforge.edge.api;

import com.streamforge.edge.simulator.VirtualSensorSimulator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.CrossOrigin;

@RestController
@RequestMapping("/api/edge")
@CrossOrigin(origins = "*")
public class EdgeControlController {
    private final VirtualSensorSimulator simulator;

    public EdgeControlController(VirtualSensorSimulator simulator) {
        this.simulator = simulator;
    }

    @GetMapping("/stats")
    public VirtualSensorSimulator.EdgeStats stats() {
        return simulator.getStats();
    }

    @PostMapping("/control")
    public VirtualSensorSimulator.EdgeStats control(
            @RequestBody EdgeControlRequest request) {
        if (request.nodeCount() != null) {
            simulator.setNodeCount(request.nodeCount());
        }
        if (request.emitIntervalMs() != null) {
            simulator.setEmitIntervalMs(request.emitIntervalMs());
        }
        if (request.deadbandEpsilon() != null) {
            simulator.setDeadbandEpsilon(request.deadbandEpsilon());
        }
        if (request.zscoreThreshold() != null) {
            simulator.setZscoreThreshold(request.zscoreThreshold());
        }
        boolean triggerAnomalySpike =
                Boolean.TRUE.equals(request.anomalyBurst())
                        || Boolean.TRUE.equals(request.triggerAnomalySpike());
        if (triggerAnomalySpike) {
            simulator.triggerAnomalyBurst(
                    request.anomalyBurstCount() == null
                            ? 20
                            : request.anomalyBurstCount());
        }
        return simulator.getStats();
    }

    public record EdgeControlRequest(
            Integer nodeCount,
            Integer emitIntervalMs,
            Double deadbandEpsilon,
            Double zscoreThreshold,
            Boolean anomalyBurst,
            Boolean triggerAnomalySpike,
            Integer anomalyBurstCount) {
    }
}
