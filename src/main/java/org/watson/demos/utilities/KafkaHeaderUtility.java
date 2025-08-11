package org.watson.demos.utilities;

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.kafka.retrytopic.RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS;
import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_CAUSE_FQCN;
import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_FQCN;
import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_MESSAGE;
import static org.springframework.kafka.support.KafkaHeaders.EXCEPTION_STACKTRACE;

@UtilityClass
public final class KafkaHeaderUtility {

    public static Optional<byte[]> lastHeader(final Headers headers, final String key) {
        return Optional.of(headers)
                .map(h -> h.lastHeader(key))
                .map(Header::value);
    }

    public static Optional<Boolean> lastHeaderAsBoolean(final Headers headers, final String key) {
        return lastHeaderAsString(headers, key)
                .map(Boolean::valueOf);
    }

    public static Optional<Integer> lastHeaderAsInteger(final Headers headers, final String key) {
        return lastHeaderAsNumber(headers, key)
                .map(Number::intValue);
    }

    public static Optional<Long> lastHeaderAsLong(final Headers headers, final String key) {
        return lastHeaderAsNumber(headers, key)
                .map(Number::longValue);
    }

    public static Optional<Number> lastHeaderAsNumber(final Headers headers, final String key) {
        return lastHeader(headers, key)
                .map(ByteBuffer::wrap)
                .map(KafkaHeaderUtility::toNumber);
    }

    public static Optional<String> lastHeaderAsString(final Headers headers, final String key) {
        return lastHeader(headers, key)
                .map(String::new);
    }

    public static Optional<Long> removeHeaderAsLong(final Headers headers, final String key) {
        final Optional<Long> result = lastHeaderAsLong(headers, key);
        headers.remove(key);
        return result;
    }

    public static Header toHeader(final String key, final byte[] value) {
        return new RecordHeader(key, value);
    }

    public static Header toHeader(final String key, final AtomicInteger value) {
        return toHeader(key, value == null ? null : value.get());
    }

    public static Header toHeader(final String key, final Boolean value) {
        return toHeader(key, value == null ? null : value.toString());
    }

    public static Header toHeader(final String key, final Integer value) {
        return toHeader(key, value, v -> ByteBuffer.allocate(Integer.BYTES).putInt(v).array());
    }

    public static Header toHeader(final String key, final Long value) {
        return toHeader(key, value, v -> ByteBuffer.allocate(Long.BYTES).putLong(v).array());
    }

    public static Header toHeader(final String key, final String value) {
        return toHeader(key, value, String::getBytes);
    }

    public static <T> Header toHeader(final String key, final T value, final Function<T, byte[]> function) {
        return toHeader(key, value == null ? null : function.apply(value));
    }

    public static Headers toHeaders(final Header... headers) {
        return new RecordHeaders(headers);
    }

    public static Headers toHeaders(final Iterable<Header> headers) {
        return new RecordHeaders(headers);
    }

    public static <K, V> ProducerRecord<K, V> toProducerRecord(final String topic, final ConsumerRecord<K, V> record, final Header... headers) {
        return toProducerRecord(topic, record.key(), record.value(), mergeHeaders(record.headers(), headers));
    }

    public static <K, V> ProducerRecord<K, V> toProducerRecord(final String topic, final K key, final V value, final Iterable<Header> headers) {
        return toProducerRecord(topic, key, value, toHeaders(headers));
    }

    public static <K, V> ProducerRecord<K, V> toProducerRecord(final String topic, final K key, final V value, final Header... headers) {
        return toProducerRecord(topic, key, value, toHeaders(headers));
    }

    public static <K, V> ProducerRecord<K, V> toProducerRecord(final String topic, final K key, final V value, final Headers headers) {
        return new ProducerRecord<>(topic, null, key, value, headers);
    }

    public static <K, V> int toRecordAttempts(final ConsumerRecord<K, V> record) {
        return lastHeaderAsInteger(record.headers(), DEFAULT_HEADER_ATTEMPTS).orElse(1);
    }

    public static <K, V> String toRecordInfoString(final ConsumerRecord<K, V> record) {
        return String.format("topic=%s, partition=%s, offset=%s, key=%s, attempts=%s",
                record.topic(), record.partition(), record.offset(), record.key(), toRecordAttempts(record));
    }

    public static <K, V> String toExceptionInfoString(final ConsumerRecord<K, V> record) {
        return Stream.of(
                        toRetryExceptionClass(record).map(v -> "exception.class=" + v),
                        toRetryExceptionMessage(record).map(v -> "exception.message=" + v),
                        toRetryExceptionCauseClass(record).map(v -> "exception.cause=" + v))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining(", ", ", ", ""));
    }

    public static <K, V> Optional<String> toRetryExceptionClass(final ConsumerRecord<K, V> record) {
        return lastHeaderAsString(record.headers(), EXCEPTION_STACKTRACE) // Parse from stacktrace
                .flatMap(KafkaExceptionUtility::findException)
                .or(() -> lastHeaderAsString(record.headers(), EXCEPTION_FQCN)); // Fallback to Spring-Kafka header
    }

    public static <K, V> Optional<String> toRetryExceptionCauseClass(final ConsumerRecord<K, V> record) {
        return lastHeaderAsString(record.headers(), EXCEPTION_STACKTRACE) // Parse from stacktrace
                .flatMap(KafkaExceptionUtility::findCause)
                .or(() -> lastHeaderAsString(record.headers(), EXCEPTION_CAUSE_FQCN)); // Fallback to Spring-Kafka header
    }

    public static <K, V> Optional<String> toRetryExceptionMessage(final ConsumerRecord<K, V> record) {
        return lastHeaderAsString(record.headers(), EXCEPTION_MESSAGE); // Spring-Kafka header
    }

    private static Headers mergeHeaders(final Headers headers, final Header... additional) {
        if (additional != null) {
            Arrays.stream(additional).forEach(headers::add);
        }
        return headers;
    }

    private static Number toNumber(final ByteBuffer byteBuffer) {
        if (byteBuffer.capacity() == Short.BYTES) {
            return byteBuffer.getShort();
        } else if (byteBuffer.capacity() == Integer.BYTES) {
            return byteBuffer.getInt();
        } else {
            return byteBuffer.getLong();
        }
    }
}
