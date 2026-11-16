package com.campuslab.bookings.service;

import com.campuslab.bookings.dto.BookingDTO;
import com.campuslab.bookings.dto.BookingStreamEventDTO;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class BookingService {

    private final List<BookingDTO> bookings = new ArrayList<>();
    private final BookingKafkaProducer bookingKafkaProducer;

    public BookingService(BookingKafkaProducer bookingKafkaProducer) {
        this.bookingKafkaProducer = bookingKafkaProducer;

        // Datos mock de prueba iniciales
        bookings.add(new BookingDTO("1", "estudiante@duocuc.cl", "LAB-101", "Laboratorio de Redes",
                LocalDateTime.now().plusDays(1), LocalDateTime.now().plusDays(1).plusHours(2), "SOLICITADA"));
        bookings.add(new BookingDTO("2", "estudiante2@duocuc.cl", "EQU-202", "Osciloscopio Digital",
                LocalDateTime.now().plusDays(2), LocalDateTime.now().plusDays(2).plusHours(1), "APROBADA"));
    }

    public List<BookingDTO> getAllBookings() {
        return bookings;
    }

    public Optional<BookingDTO> getBookingById(String id) {
        return bookings.stream().filter(b -> b.getId().equals(id)).findFirst();
    }

    public BookingDTO createBooking(BookingDTO booking) {
        booking.setId(UUID.randomUUID().toString());
        if (booking.getStatus() == null) {
            booking.setStatus("SOLICITADA");
        }
        bookings.add(booking);

        // Publicar evento en Apache Kafka (campuslab.bookings.stream)
        publishToKafka("BOOKING_CREATED", booking);

        return booking;
    }

    public Optional<BookingDTO> updateStatus(String id, String newStatus) {
        Optional<BookingDTO> bookingOpt = getBookingById(id);
        bookingOpt.ifPresent(b -> {
            b.setStatus(newStatus);
            // Publicar actualización de estado en Apache Kafka
            publishToKafka("BOOKING_STATUS_UPDATED", b);
        });
        return bookingOpt;
    }

    private void publishToKafka(String eventType, BookingDTO booking) {
        BookingStreamEventDTO event = BookingStreamEventDTO.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(eventType)
                .bookingId(booking.getId())
                .resourceId(booking.getResourceId())
                .userId(booking.getUserEmail())
                .status(booking.getStatus())
                .timestamp(LocalDateTime.now())
                .build();

        bookingKafkaProducer.publishBookingEvent(event);
    }
}