package org.watson.demos.utilities;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static java.util.function.Predicate.not;
import static org.springframework.kafka.retrytopic.RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS;
import static org.springframework.kafka.retrytopic.RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP;
import static org.springframework.kafka.retrytopic.RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP;
import static org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN;
import static org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_FQCN;
import static org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_MESSAGE;
import static org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_STACKTRACE;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_CONSUMER_GROUP;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_OFFSET;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_TIMESTAMP;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_TIMESTAMP_TYPE;
import static org.springframework.kafka.support.KafkaHeaders.ORIGINAL_TOPIC;

@Slf4j
@UtilityClass
public class KafkaHeaderParser {
    private static final Function<byte[], Object> CONVERTER_BIGINT_LONG = ((Function<byte[], BigInteger>) BigInteger::new).andThen(BigInteger::longValue);
    private static final Function<byte[], Object> CONVERTER_INT = ((Function<byte[], ByteBuffer>) ByteBuffer::wrap).andThen(ByteBuffer::getInt);
    private static final Function<byte[], Object> CONVERTER_LONG = ((Function<byte[], ByteBuffer>) ByteBuffer::wrap).andThen(ByteBuffer::getLong);
    private static final Function<byte[], Object> CONVERTER_STRING = String::new;
    private static final Map<Class<?>, Function<byte[], ?>> CLASS_MAPPERS = Map.ofEntries(
            Map.entry(BigInteger.class, CONVERTER_BIGINT_LONG),
            Map.entry(Integer.class, CONVERTER_INT),
            Map.entry(Long.class, CONVERTER_LONG),
            Map.entry(String.class, CONVERTER_STRING)
    );
    private static final Map<String, Function<byte[], Object>> KAFKA_HEADER_MAPPERS = Map.ofEntries(
            Map.entry(DEFAULT_HEADER_ATTEMPTS, CONVERTER_INT),
            Map.entry(DEFAULT_HEADER_BACKOFF_TIMESTAMP, CONVERTER_BIGINT_LONG),
            Map.entry(DEFAULT_HEADER_ORIGINAL_TIMESTAMP, CONVERTER_BIGINT_LONG),
            Map.entry(DLT_EXCEPTION_CAUSE_FQCN, CONVERTER_STRING),
            Map.entry(DLT_EXCEPTION_FQCN, CONVERTER_STRING),
            Map.entry(DLT_EXCEPTION_MESSAGE, CONVERTER_STRING),
            Map.entry(DLT_EXCEPTION_STACKTRACE, CONVERTER_STRING),
            Map.entry(ORIGINAL_CONSUMER_GROUP, CONVERTER_STRING),
            Map.entry(ORIGINAL_OFFSET, CONVERTER_LONG),
            Map.entry(ORIGINAL_PARTITION, CONVERTER_INT),
            Map.entry(ORIGINAL_TIMESTAMP, CONVERTER_BIGINT_LONG),
            Map.entry(ORIGINAL_TIMESTAMP_TYPE, CONVERTER_STRING),
            Map.entry(ORIGINAL_TOPIC, CONVERTER_STRING)
    );
    private static final Predicate<Header> EXCLUDE_FILTER = h -> h.key() == null || h.key().contains("trace");

    public static Map<String, Object> parseHeaders(final Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
                .map(KafkaHeaderParser::tryParseHeader)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
    }

    @SuppressWarnings("unchecked")
    public static <T> Optional<T> getHeaderValue(final Headers headers, final String key) {
        return getHeaderValue(headers, key, KAFKA_HEADER_MAPPERS.getOrDefault(key, CONVERTER_STRING))
                .map(tryOrNull(v -> (T) v));
    }

    public static <T> Optional<T> getHeaderValue(final Headers headers, final String key, final Class<T> clazz) {
        return getHeaderValue(headers, key, CLASS_MAPPERS.getOrDefault(clazz, CONVERTER_STRING))
                .map(tryOrNull(clazz::cast));
    }

    private static <T> Optional<T> getHeaderValue(final Headers headers, final String key, final Function<byte[], T> converter) {
        return Optional.of(headers)
                .map(h -> h.lastHeader(key))
                .map(Header::value)
                .map(tryOrNull(converter));
    }

    private Optional<Map.Entry<String, Object>> tryParseHeader(final Header header) {
        try {
            return Optional.of(header)
                    .filter(not(EXCLUDE_FILTER))
                    .map(Header::value)
                    .map(v -> KAFKA_HEADER_MAPPERS.getOrDefault(header.key(), CONVERTER_STRING).apply(v))
                    .map(v -> Map.entry(header.key(), v));
        } catch (RuntimeException e) {
            log.debug("Unable to parse header. key={}, exceptionClass={}, message={}", header.key(), e.getClass().getSimpleName(), e.getMessage());
            return Optional.empty();
        }
    }

    private static <T, R> Function<T, R> tryOrNull(final Function<T, R> function) {
        return v -> {
            try {
                return function.apply(v);
            } catch (Exception e) {
                log.debug("Unable to parse header. exceptionClass={}, message={}", e.getClass().getSimpleName(), e.getMessage());
                return null;
            }
        };
    }
}
