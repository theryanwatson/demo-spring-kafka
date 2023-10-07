package org.watson.demos.configurations;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

@ConditionalOnProperty("spring.kafka.retry.topic.enabled")
@RequiredArgsConstructor
@EnableKafka
@EnableScheduling
@Configuration(proxyBeanMethods = false)
public class KafkaRetryConfiguration extends RetryTopicConfigurationSupport {
    @Value("${spring.kafka.retry.log.exception.stack:false}")
    private final boolean logExceptionStack;
    @Value("${spring.kafka.retry.retain.all.header:false}")
    private final boolean retainAllRetryHeaderValues;
    @Value("${spring.kafka.retry.auto.create.topics:false}")
    private final boolean autoCreateTopics;

    @Bean
    public RetryTopicConfiguration myRetryTopic(final KafkaTemplate<String, Object> template,
                                                @Value("${spring.kafka.retry.topic.delay:}") final Optional<Duration> delay,
                                                @Value("${spring.kafka.retry.topic.attempts:}") final Optional<Integer> attempts,
                                                @Value("${spring.kafka.retry.topic.max-delay:}") final Optional<Duration> maxDelay,
                                                @Value("${spring.kafka.retry.topic.multiplier:}") final Optional<Integer> multiplier,
                                                @Value("${spring.kafka.retry.topic.random-back-off:}") final Optional<Boolean> randomBackoff,
                                                @Value("${spring.kafka.retry.auto.create.topics:false}") final boolean autoCreateTopics,
                                                @Value("${spring.kafka.retry.auto.create.topics.partition.count:-1}") final int numPartitions,
                                                @Value("${spring.kafka.retry.auto.create.topics.replication.factor:-1}") final short replicationFactor,
                                                @Value("${spring.kafka.retry.fixed.delay.strategy:SINGLE_TOPIC}") final FixedDelayStrategy fixedDelayStrategy) {
        final RetryTopicConfigurationBuilder builder = RetryTopicConfigurationBuilder.newInstance();
        attempts.ifPresent(builder::maxAttempts);
        setBackoff(builder, delay, maxDelay, multiplier, randomBackoff);

        return builder
                .autoCreateTopics(autoCreateTopics, numPartitions, replicationFactor)
                .useSingleTopicForFixedDelays(fixedDelayStrategy)
                .create(template);
    }

    @Override
    protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
        return factory -> {
            if (!logExceptionStack) {
                factory.neverLogListenerException();
            }
            if (!autoCreateTopics) {
                factory.setPartitionResolver((cr, nextTopic) -> -1);
            }
            factory.setRetainAllRetryHeaderValues(retainAllRetryHeaderValues);
        };
    }

    private static void setBackoff(final RetryTopicConfigurationBuilder builder, final Optional<Duration> delay, final Optional<Duration> maxDelay, final Optional<Integer> multiplier, final Optional<Boolean> randomBackoff) {
        if (delay.isPresent()) {
            if (maxDelay.isEmpty()) {
                builder.fixedBackOff(delay.get().toMillis());
            } else if (multiplier.isEmpty()) {
                builder.uniformRandomBackoff(delay.get().toMillis(), maxDelay.get().toMillis());
            } else {
                builder.exponentialBackoff(delay.get().toMillis(), maxDelay.get().toMillis(), multiplier.get(), randomBackoff.orElse(false));
            }
        }
    }
}
