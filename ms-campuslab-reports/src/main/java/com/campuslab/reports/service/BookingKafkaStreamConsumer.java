package com.campuslab.reports.service;

import com.campuslab.reports.dto.AnalyticsMetricDTO;
import com.campuslab.reports.dto.BookingStreamEventDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class BookingKafkaStreamConsumer {

    private static final String BOOKINGS_STREAM_TOPIC = "campuslab.bookings.stream";
    private static final String ANALYTICS_METRICS_TOPIC = "campuslab.analytics.metrics";

    private final KafkaTemplate<String, AnalyticsMetricDTO> kafkaTemplate;
    private final AtomicLong processedCounter = new AtomicLong(0);

    @KafkaListener(
            topics = BOOKINGS_STREAM_TOPIC,
            groupId = "reports-analytics-group"
    )
    public void consumeBookingStream(
            @Payload BookingStreamEventDTO event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        // Validación defensiva de payload (Rúbrica: manejo de errores)
        if (event == null || event.getBookingId() == null || event.getResourceId() == null) {
            log.error("[KAFKA-CONSUMER-REPORTS] Payload inválido recibido en partición {}, offset {}: {}",
                    partition, offset, event);
            throw new IllegalArgumentException("Evento de reserva incompleto o nulo");
        }

        long currentCount = processedCounter.incrementAndGet();
        log.info("[KAFKA-CONSUMER-REPORTS] Evento procesado #{} | Tipo: {} | Reserva: {} | Recurso: {} | Partición: {} | Offset: {}",
                currentCount, event.getEventType(), event.getBookingId(), event.getResourceId(), partition, offset);

        // Generar y publicar métrica agregada al segundo tópico (campuslab.analytics.metrics)
        AnalyticsMetricDTO metric = AnalyticsMetricDTO.builder()
                .metricId(UUID.randomUUID().toString())
                .resourceId(event.getResourceId())
                .eventType(event.getEventType())
                .totalEventsProcessed(currentCount)
                .calculatedAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send(ANALYTICS_METRICS_TOPIC, event.getResourceId(), metric)
                .whenComplete((res, ex) -> {
                    if (ex == null) {
                        log.info("[KAFKA-PRODUCER-REPORTS] Métrica analítica publicada en '{}' para recurso {}",
                                ANALYTICS_METRICS_TOPIC, event.getResourceId());
                    } else {
                        log.error("[KAFKA-PRODUCER-REPORTS] Fallo al publicar métrica analítica: {}", ex.getMessage());
                    }
                });
    }
}