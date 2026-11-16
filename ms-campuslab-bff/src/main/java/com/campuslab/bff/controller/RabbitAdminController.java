package com.campuslab.bff.controller;

import org.springframework.amqp.core.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/admin/rabbitmq")
public class RabbitAdminController {

    private final AmqpAdmin amqpAdmin;

    public RabbitAdminController(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    // Crear Cola
    @PostMapping("/queues/{name}")
    public ResponseEntity<String> createQueue(@PathVariable String name) {
        amqpAdmin.declareQueue(new Queue(name, true));
        return ResponseEntity.ok("Cola '" + name + "' creada correctamente.");
    }

    // Eliminar Cola
    @DeleteMapping("/queues/{name}")
    public ResponseEntity<String> deleteQueue(@PathVariable String name) {
        amqpAdmin.deleteQueue(name);
        return ResponseEntity.ok("Cola '" + name + "' eliminada correctamente.");
    }

    // Crear Exchange
    @PostMapping("/exchanges/{name}")
    public ResponseEntity<String> createExchange(@PathVariable String name, @RequestParam(defaultValue = "topic") String type) {
        amqpAdmin.declareExchange(new TopicExchange(name));
        return ResponseEntity.ok("Exchange '" + name + "' creado correctamente.");
    }

    // Eliminar Exchange
    @DeleteMapping("/exchanges/{name}")
    public ResponseEntity<String> deleteExchange(@PathVariable String name) {
        amqpAdmin.deleteExchange(name);
        return ResponseEntity.ok("Exchange '" + name + "' eliminado correctamente.");
    }

    // Crear Binding (Unión entre Cola y Exchange)
    @PostMapping("/bindings")
    public ResponseEntity<String> createBinding(@RequestParam String queue, @RequestParam String exchange, @RequestParam String routingKey) {
        Binding binding = new Binding(queue, Binding.DestinationType.QUEUE, exchange, routingKey, null);
        amqpAdmin.declareBinding(binding);
        return ResponseEntity.ok("Binding creado entre la cola '" + queue + "' y el exchange '" + exchange + "'.");
    }
}