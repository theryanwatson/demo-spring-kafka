package org.watson.demos.listeners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.cache.Cache;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RetryableListenerConfigBased extends FakeFailingListener {
    public RetryableListenerConfigBased(final Cache messageCache) {
        super(messageCache, log);
    }

    @KafkaListener(topics = "${kafka.topic.inbound.two}", groupId = "${spring.application.name}-retryable")
    public void handleRecord(ConsumerRecord<String, String> record) {
        super.handleRecord(record);
    }
}
