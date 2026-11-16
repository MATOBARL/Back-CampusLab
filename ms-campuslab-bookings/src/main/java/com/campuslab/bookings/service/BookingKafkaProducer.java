package com.campuslab.bookings.service;

import com.campuslab.bookings.config.KafkaTopicConfig;
import com.campuslab.bookings.dto.BookingStreamEventDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class BookingKafkaProducer {

    private final KafkaTemplate<String, BookingStreamEventDTO> kafkaTemplate;

    public void publishBookingEvent(BookingStreamEventDTO event) {
        if (event == null || event.getBookingId() == null) {
            log.warn("Intento de publicar evento Kafka inválido o sin bookingId: {}", event);
            return;
        }

        String key = String.valueOf(event.getBookingId());

        kafkaTemplate.send(KafkaTopicConfig.BOOKINGS_STREAM_TOPIC, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("[KAFKA-PRODUCER] Evento {} publicado en tópico '{}' | Partición: {} | Offset: {}",
                                event.getEventType(),
                                KafkaTopicConfig.BOOKINGS_STREAM_TOPIC,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("[KAFKA-PRODUCER] Error al publicar evento {} para reserva {}: {}",
                                event.getEventType(), key, ex.getMessage(), ex);
                    }
                });
    }
}