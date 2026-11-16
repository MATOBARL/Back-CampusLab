package com.campuslab.bff.service;

import com.campuslab.bff.dto.AnalyticsMetricDTO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaAdminMonitorService {

    private static final String ANALYTICS_METRICS_TOPIC = "campuslab.analytics.metrics";

    private final KafkaAdmin kafkaAdmin;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Deque<AnalyticsMetricDTO> recentMetrics = new ConcurrentLinkedDeque<>();

    @Data
    @AllArgsConstructor
    public static class TopicSummary {
        private String name;
        private int partitionsCount;
        private int replicationFactor;
    }

    @KafkaListener(
            topics = ANALYTICS_METRICS_TOPIC,
            groupId = "bff-analytics-group"
    )
    public void consumeAnalyticsMetrics(AnalyticsMetricDTO metric) {
        if (metric == null) {
            return;
        }
        log.info("[KAFKA-CONSUMER-BFF] Métrica recibida | Recurso: {} | Total eventos: {}",
                metric.getResourceId(), metric.getTotalEventsProcessed());

        recentMetrics.addFirst(metric);
        while (recentMetrics.size() > 50) {
            recentMetrics.removeLast();
        }
    }

    public List<AnalyticsMetricDTO> getRecentMetrics() {
        return new ArrayList<>(recentMetrics);
    }

    public Map<String, Object> getClusterTopicsSummary() throws Exception {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> topicNames = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
            Map<String, TopicDescription> descriptions = adminClient.describeTopics(topicNames)
                    .allTopicNames()
                    .get(5, TimeUnit.SECONDS);

            List<TopicSummary> topicsInfo = new ArrayList<>();
            for (TopicDescription desc : descriptions.values()) {
                if (desc.isInternal()) {
                    continue;
                }
                int replicas = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();
                topicsInfo.add(new TopicSummary(desc.name(), desc.partitions().size(), replicas));
            }

            Collection<GroupListing> groups = adminClient.listGroups()
                    .all()
                    .get(5, TimeUnit.SECONDS);

            List<String> groupIds = new ArrayList<>();
            for (GroupListing group : groups) {
                groupIds.add(group.groupId());
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("timestamp", LocalDateTime.now());
            result.put("topics", topicsInfo);
            result.put("consumerGroups", groupIds);
            return result;
        }
    }

    public void publishTestEvent(String topic, String key, Map<String, Object> payload) {
        kafkaTemplate.send(topic, key, payload);
        log.info("[KAFKA-ADMIN-BFF] Mensaje de prueba enviado al tópico '{}' con clave '{}'", topic, key);
    }
}