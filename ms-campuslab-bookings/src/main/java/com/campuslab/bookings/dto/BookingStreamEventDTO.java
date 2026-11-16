package com.campuslab.bookings.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BookingStreamEventDTO {
    private String eventId;
    private String eventType; // BOOKING_CREATED, BOOKING_STATUS_UPDATED
    private String bookingId;
    private String resourceId;
    private String userId;
    private String status;
    private LocalDateTime timestamp;
}