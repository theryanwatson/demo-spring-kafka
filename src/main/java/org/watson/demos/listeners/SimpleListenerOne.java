package org.watson.demos.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SimpleListenerOne {

    @KafkaListener(topics = "${kafka.topic.inbound.one}")
    public void handleRecord(final ConsumerRecord<String, String> record) {
        log.debug("topic={}, partition={}, offset={}, key={}, value={}, headers={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.headers().toArray());
    }
}
