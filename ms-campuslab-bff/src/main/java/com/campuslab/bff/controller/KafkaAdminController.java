package com.campuslab.bff.controller;

import com.campuslab.bff.service.KafkaAdminMonitorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/bff/kafka")
@RequiredArgsConstructor
public class KafkaAdminController {

    private final KafkaAdminMonitorService kafkaAdminMonitorService;

    @GetMapping("/cluster")
    public ResponseEntity<Object> getClusterStatus() {
        try {
            return ResponseEntity.ok(kafkaAdminMonitorService.getClusterTopicsSummary());
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "No se pudo consultar el clúster Kafka: " + e.getMessage()));
        }
    }

    @GetMapping("/metrics")
    public ResponseEntity<Object> getAnalyticsMetrics() {
        return ResponseEntity.ok(kafkaAdminMonitorService.getRecentMetrics());
    }

    @PostMapping("/publish/{topic}")
    public ResponseEntity<Object> publishTestMessage(
            @PathVariable String topic,
            @RequestParam(defaultValue = "test-key") String key,
            @RequestBody Map<String, Object> payload) {
        kafkaAdminMonitorService.publishTestEvent(topic, key, payload);
        return ResponseEntity.ok(Map.of(
                "status", "PUBLISHED",
                "topic", topic,
                "key", key
        ));
    }
}