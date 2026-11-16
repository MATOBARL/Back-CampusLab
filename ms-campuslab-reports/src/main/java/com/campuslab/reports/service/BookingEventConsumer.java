package com.campuslab.reports.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class BookingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookingEventConsumer.class);

    @RabbitListener(queues = "campuslab.bookings.queue")
    public void processBookingEvent(Map<String, Object> event) {
        log.info("Mensaje recibido exitosamente desde la cola: {}", event);
        // Si ocurre un error inesperado, el mensaje se enviará automáticamente a la DLQ
    }

    @RabbitListener(queues = "campuslab.bookings.dlq")
    public void processDeadLetterMessage(Map<String, Object> failedEvent) {
        log.error("ALERT: Mensaje no entregado redirigido a DLQ para auditoría: {}", failedEvent);
    }
}