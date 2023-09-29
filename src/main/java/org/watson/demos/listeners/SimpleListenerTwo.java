package org.watson.demos.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
@Component
@RequiredArgsConstructor
public class SimpleListenerTwo {

    @KafkaListener(topics = "${kafka.topic.inbound.two}")
    public void handleRecord(final ConsumerRecord<String, String> record) {
        log.debug("topic={}, partition={}, offset={}, key={}, value={}, headers={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), StreamSupport.stream(record.headers().spliterator(), false).map(Header::key).sorted().collect(Collectors.toList()));
    }
}
