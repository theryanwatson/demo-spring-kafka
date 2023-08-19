package org.watson.demos.controllers;

import io.micrometer.core.annotation.Timed;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RequiredArgsConstructor
@Timed(value = "http.producer.requests", extraTags = {"version", "1"}, description = "/producer")
@RequestMapping(path = "${spring.data.rest.base-path:}/producer", produces = MediaType.APPLICATION_JSON_VALUE)
@RestController
public class ProducerController {

    private final KafkaTemplate<String, String> producer;
    @Value("${kafka.topic.inbound.one}")
    private final String topicOne;
    @Value("${kafka.topic.inbound.two}")
    private final String topicTwo;

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PutMapping("/one")
    public void produceToOne(String message) {
        producer.send(topicOne, message);
    }

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PutMapping("/two")
    public void produceToTwo(String message) {
        producer.send(topicTwo, message);
    }
}
