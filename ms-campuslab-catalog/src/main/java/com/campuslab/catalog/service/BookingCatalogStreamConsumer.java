package com.campuslab.catalog.service;

import com.campuslab.catalog.dto.BookingStreamEventDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class BookingCatalogStreamConsumer {

    private static final String BOOKINGS_STREAM_TOPIC = "campuslab.bookings.stream";

    @KafkaListener(
            topics = BOOKINGS_STREAM_TOPIC,
            groupId = "catalog-availability-group"
    )
    public void onBookingEventReceived(
            @Payload BookingStreamEventDTO event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        if (event == null || event.getResourceId() == null) {
            log.warn("[KAFKA-CONSUMER-CATALOG] Evento inválido ignorado en partición {}, offset {}", partition, offset);
            return;
        }

        log.info("[KAFKA-CONSUMER-CATALOG] Sincronizando disponibilidad de recurso '{}' | Evento: {} | Estado reserva: {} | Partición: {} | Offset: {}",
                event.getResourceId(), event.getEventType(), event.getStatus(), partition, offset);
    }
}