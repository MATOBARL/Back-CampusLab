package com.campuslab.reports.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BookingStreamEventDTO {
    private String eventId;
    private String eventType;
    private String bookingId;
    private String resourceId;
    private String userId;
    private String status;
    private LocalDateTime timestamp;
}