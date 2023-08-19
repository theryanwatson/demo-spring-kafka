package org.watson.demos.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TwoListener {

    @KafkaListener(topics = "${kafka.topic.inbound.two}")
    public void handleRecord(final ConsumerRecord<String, String> record) {
        if (record.value() != null) {
            log.debug("{}, key={}", record.value(), record.key());
        }
    }
}
