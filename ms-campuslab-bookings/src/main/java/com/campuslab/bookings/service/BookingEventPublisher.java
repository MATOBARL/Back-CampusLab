package com.campuslab.bookings.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class BookingEventPublisher {

    private final RabbitTemplate rabbitTemplate;

    public BookingEventPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void publishBookingCreated(Map<String, Object> bookingEvent) {
        rabbitTemplate.convertAndSend("campuslab.exchange", "booking.created", bookingEvent);
    }
}