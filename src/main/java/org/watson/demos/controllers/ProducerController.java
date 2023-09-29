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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.IntStream;

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
    public void produceToOne(final String key, final String message, @RequestParam(required = false, defaultValue = "1") final Integer count) {
        IntStream.range(0, count)
                .forEach(i -> producer.send(topicOne, key + i, message + i));
    }

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PutMapping("/two")
    public void produceToTwo(final String key, final String message, @RequestParam(required = false, defaultValue = "1") final Integer count) {
        IntStream.range(0, count)
                .forEach(i -> producer.send(topicTwo, key + i, message + i));
    }
}
