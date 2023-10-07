package org.watson.demos.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static org.watson.demos.utilities.KafkaHeaderParser.parseHeaders;

@Slf4j
@Component
@RequiredArgsConstructor
public class SimpleListenerTwo {

    @KafkaListener(topics = "${kafka.topic.inbound.two}")
    public void handleRecord(final ConsumerRecord<String, String> record) {
        log.debug("topic={}, partition={}, offset={}, key={}, value={}, headers={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), parseHeaders(record.headers()));
    }
}
