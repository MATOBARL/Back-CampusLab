package com.campuslab.reports.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        // 3 reintentos separados por 2000 ms (2 segundos)
        FixedBackOff backOff = new FixedBackOff(2000L, 3L);

        return new DefaultErrorHandler((consumerRecord, exception) -> {
            log.error("[KAFKA-ERROR-HANDLER] Mensaje descartado tras agotar reintentos | Tópico: {} | Partición: {} | Offset: {} | Payload: {} | Causa: {}",
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.value(),
                    exception.getMessage());
        }, backOff);
    }
}