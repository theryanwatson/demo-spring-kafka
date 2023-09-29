package org.watson.demos.listeners;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

@Slf4j
@RequiredArgsConstructor
@Component
public class RelaySleepListener {
    private final KafkaTemplate<String, String> kafkaTemplate;
    @Value("${kafka.topic.inbound.two}")
    private final String output;
    @Value("PT${relay.delay.duration:60S}")
    private final Duration delayDuration;
    @Value("${relay.retry.clock:#{T(java.time.Clock).systemUTC()}}")
    private final Clock clock;
    @Value("${relay.retry.header.name:x-relay-retry-count}")
    private final String retryHeader;

    @KafkaListener(containerFactory = "batchListenerFactory", topics = "${kafka.topic.inbound.one}", groupId = "${spring.application.name}-relay")
    public void handleRecord(final ConsumerRecords<String, String> consumerRecords, final Consumer<?, ?> consumer) {
        final long maxTimestamp = StreamSupport.stream(consumerRecords.spliterator(), false)
                .mapToLong(ConsumerRecord::timestamp)
                .max().orElse(0);

        final long actualDelayMs = Math.min(maxTimestamp - clock.millis(), 0) + delayDuration.toMillis();
        if (actualDelayMs > 0) {
            log.info("messageCount={}, delayMs={}", consumerRecords.count(), actualDelayMs);
            consumer.pause(consumerRecords.partitions());
            delay(actualDelayMs);
        }

        consumerRecords.forEach(m -> kafkaTemplate.send(new ProducerRecord<>(output, null, m.key(), m.value(), incrementRetryCount(m.headers()))));
        consumer.resume(consumerRecords.partitions());
    }

    private Iterable<Header> incrementRetryCount(Headers headers) {
        final int retryCount = Optional.of(headers)
                .map(h -> h.lastHeader(retryHeader))
                .map(Header::value)
                .map(ByteBuffer::wrap)
                .map(ByteBuffer::getInt)
                .map(i -> ++i)
                .orElse(1);

        return List.of(new RecordHeader(retryHeader, ByteBuffer.allocate(Integer.BYTES).putInt(retryCount).array()));
    }

    @SneakyThrows
    void delay(long delayMs) {
        Thread.sleep(delayMs);
    }
}
