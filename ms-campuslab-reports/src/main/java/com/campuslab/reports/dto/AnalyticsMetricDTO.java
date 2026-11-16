package com.campuslab.reports.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnalyticsMetricDTO {
    private String metricId;
    private String resourceId;
    private String eventType;
    private long totalEventsProcessed;
    private LocalDateTime calculatedAt;
}