package com.campuslab.bookings.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String BOOKINGS_STREAM_TOPIC = "campuslab.bookings.stream";
    public static final String ANALYTICS_METRICS_TOPIC = "campuslab.analytics.metrics";

    @Bean
    public NewTopic bookingsStreamTopic() {
        return TopicBuilder.name(BOOKINGS_STREAM_TOPIC)
                .partitions(3)
                .replicas(1)
                // Retención de 7 días (en milisegundos) y política delete
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    @Bean
    public NewTopic analyticsMetricsTopic() {
        return TopicBuilder.name(ANALYTICS_METRICS_TOPIC)
                .partitions(2)
                .replicas(1)
                // Retención de 30 días y política compact,delete para métricas analíticas
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete")
                .build();
    }
}