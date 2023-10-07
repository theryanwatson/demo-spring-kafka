package org.watson.demos.listeners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.cache.Cache;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.watson.demos.exceptions.DoNotRetryException;

@Slf4j
@Component
public class RetryableListenerAnnotation extends FakeFailingListener {
    public RetryableListenerAnnotation(final Cache messageCache) {
        super(messageCache, log);
    }

    @Override
    @RetryableTopic(attempts = "${spring.kafka.retry.topic.attempts:3}",
            autoCreateTopics = "${spring.kafka.retry.auto.create.topics:false}",
            backoff = @Backoff(delayExpression = "${spring.kafka.retry.topic.delay.ms:1000}"),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC,
            numPartitions = "${spring.kafka.retry.auto.create.topics.partition.count:-1}",
            replicationFactor = "${spring.kafka.retry.auto.create.topics.replication.factor:-1}",
            exclude = {DoNotRetryException.class})
    @KafkaListener(topics = "${kafka.topic.inbound.one}", groupId = "${spring.application.name}-retryable")
    public void handleRecord(ConsumerRecord<String, String> record) {
        super.handleRecord(record);
    }
}
