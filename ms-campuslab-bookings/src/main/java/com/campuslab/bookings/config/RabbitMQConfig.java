package com.campuslab.bookings.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String MAIN_QUEUE = "campuslab.bookings.queue";
    public static final String DLQ_QUEUE = "campuslab.bookings.dlq";
    public static final String EXCHANGE = "campuslab.exchange";
    public static final String DLX_EXCHANGE = "campuslab.dlx.exchange";

    // 1. Exchange Principal y DLX
    @Bean
    public TopicExchange mainExchange() {
        return new TopicExchange(EXCHANGE);
    }

    @Bean
    public TopicExchange deadLetterExchange() {
        return new TopicExchange(DLX_EXCHANGE);
    }

    // 2. Cola Principal con redirección a DLQ en caso de fallo
    @Bean
    public Queue mainQueue() {
        return QueueBuilder.durable(MAIN_QUEUE)
                .withArgument("x-dead-letter-exchange", DLX_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", "booking.deadletter")
                .build();
    }

    // 3. Cola DLQ
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ_QUEUE).build();
    }

    // 4. Bindings
    @Bean
    public Binding mainBinding() {
        return BindingBuilder.bind(mainQueue()).to(mainExchange()).with("booking.#");
    }

    @Bean
    public Binding dlqBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange()).with("booking.deadletter");
    }
}