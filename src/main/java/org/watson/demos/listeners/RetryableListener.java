package org.watson.demos.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.cache.Cache;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_TOPIC;

@Slf4j
@RequiredArgsConstructor
@Component
public class RetryableListener {
    private final Cache messageCache;

    @RetryableTopic(attempts = "${spring.kafka.retry.topic.attempts:3}", backoff = @Backoff(delayExpression = "${spring.kafka.retry.topic.delay.ms:10000}"),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE, fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC)
    @KafkaListener(topics = "${kafka.topic.inbound.one}", groupId = "${spring.application.name}-retryable")
    public void handleRecord(final ConsumerRecord<String, String> record) {
        log.debug("topic={}, partition={}, offset={}, key={}, value={}, headers={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), StreamSupport.stream(record.headers().spliterator(), false).map(Header::key).sorted().collect(Collectors.toList()));

        final String recordKey = getRecordKey(record);
        if (getFakeFailCount(recordKey).getAndDecrement() > 0) {
            throw new RuntimeException("Fake fail " + recordKey);
        }
    }

    @DltHandler
    public void handleDlt(final ConsumerRecord<String, String> record) {
        log.debug("dlt={}", getRecordKey(record));
    }

    private AtomicInteger getFakeFailCount(String key) {
        return messageCache.get(key, () -> {
            final AtomicInteger failCount = new AtomicInteger(RandomUtils.nextInt(1, 4));
            log.debug("Setting fake fail. key={}, count={}", key, failCount);
            return failCount;
        });
    }

    private static String getRecordKey(final ConsumerRecord<String, String> record) {
        final String topic = getHeaderOr(record.headers(), ORIGINAL_TOPIC, b -> new String(b.array()), record::topic);
        final String offset = getHeaderOr(record.headers(), ORIGINAL_OFFSET, ByteBuffer::getLong, record::offset);
        final String partition = getHeaderOr(record.headers(), ORIGINAL_PARTITION, ByteBuffer::getInt, record::partition);
        return String.join("::", topic, offset, partition, record.key());
    }

    private static <T> String getHeaderOr(final Headers headers, final String key, final Function<ByteBuffer, T> converter, final Supplier<T> supplier) {
        return String.valueOf(Optional.of(headers)
                .map(h -> h.lastHeader(key))
                .map(Header::value)
                .map(ByteBuffer::wrap)
                .map(converter)
                .orElseGet(supplier));
    }
}
