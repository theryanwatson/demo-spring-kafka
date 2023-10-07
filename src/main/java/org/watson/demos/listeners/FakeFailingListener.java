package org.watson.demos.listeners;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.cache.Cache;
import org.watson.demos.exceptions.DoNotRetryException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.murmur2;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_TOPIC;
import static org.watson.demos.utilities.KafkaHeaderParser.getHeaderValue;
import static org.watson.demos.utilities.KafkaHeaderParser.parseHeaders;

@RequiredArgsConstructor
public class FakeFailingListener {
    private final Cache messageCache;
    private final Logger log;

    public void handleRecord(final ConsumerRecord<String, String> record) {
        log.debug("topic={}, partition={}, offset={}, key={}, value={}, headers={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), parseHeaders(record.headers()));

        final String recordKey = getRecordKey(record);
        if (getFakeFailCount(recordKey).getAndDecrement() > 0) {
            if (murmur2(recordKey.getBytes()) % 3 == 0) {
                throw new DoNotRetryException(getClass().getSimpleName() + " Do not retry fake fail " + recordKey);
            }
            throw new RuntimeException(getClass().getSimpleName() + " Fake fail " + recordKey);
        }
    }

    private AtomicInteger getFakeFailCount(String key) {
        return messageCache.get(key, () -> {
            final AtomicInteger failCount = new AtomicInteger(RandomUtils.nextInt(1, 4));
            log.debug("Setting fake fail. key={}, count={}", key, failCount);
            return failCount;
        });
    }

    private static String getRecordKey(final ConsumerRecord<String, String> record) {
        return Stream.of(getHeaderValue(record.headers(), ORIGINAL_TOPIC).orElseGet(record::topic),
                        getHeaderValue(record.headers(), ORIGINAL_OFFSET).orElseGet(record::offset),
                        getHeaderValue(record.headers(), ORIGINAL_PARTITION).orElseGet(record::partition))
                .map(String::valueOf)
                .collect(Collectors.joining("::"));
    }
}
