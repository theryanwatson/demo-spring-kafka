package org.watson.demos.configurations;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * Configuration to make Kafka Retry modifiable from properties:
 * <table>
 *     <tr><th>Property Name</th>                                   <th>Default Value</th></tr>
 *     <tr><td>spring.kafka.retry.topic.attempts</td>               <td>3</td></tr>
 *     <tr><td>spring.kafka.retry.topic.auto-create-topics</td>     <td>false</td></tr>
 *     <tr><td>spring.kafka.retry.topic.delay</td>                  <td>10s</td></tr>
 *     <tr><td>spring.kafka.retry.topic.dlt-strategy</td>           <td>null</td></tr>
 *     <tr><td>spring.kafka.retry.topic.enabled</td>                <td>null</td></tr>
 *     <tr><td>spring.kafka.retry.topic.fixed-delay-strategy</td>   <td>SINGLE_TOPIC</td></tr>
 *     <tr><td>spring.kafka.retry.topic.log-exception-stack</td>    <td>false</td></tr>
 *     <tr><td>spring.kafka.retry.topic.max-delay</td>              <td>null</td></tr>
 *     <tr><td>spring.kafka.retry.topic.multiplier</td>             <td>null</td></tr>
 *     <tr><td>spring.kafka.retry.topic.not-retry-on</td>           <td>null</td></tr>
 *     <tr><td>spring.kafka.retry.topic.num-partitions</td>         <td>-1</td></tr>
 *     <tr><td>spring.kafka.retry.topic.random-backoff</td>         <td>false</td></tr>
 *     <tr><td>spring.kafka.retry.topic.replication-factor</td>     <td>-1</td></tr>
 *     <tr><td>spring.kafka.retry.topic.retain-all-headers</td>     <td>false</td></tr>
 *     <tr><td>spring.kafka.retry.topic.topic-suffix-strategy</td>  <td>null</td></tr>
 * </table>
 */
@ConditionalOnProperty("spring.kafka.retry.topic.enabled")
@RequiredArgsConstructor
@EnableKafka
@EnableScheduling
@Configuration
public class KafkaRetryConfiguration extends RetryTopicConfigurationSupport {
    @Value("${spring.kafka.retry.topic.log-exception-stack:false}")
    private final boolean logExceptionStack;
    @Value("${spring.kafka.retry.topic.auto-create-topics:false}")
    private final boolean autoCreateTopics;
    @Value("${spring.kafka.retry.topic.retain-all-headers:false}")
    private final boolean retainAllRetryHeaderValues;

    @Bean
    public RetryTopicConfiguration retryTopicConfiguration(final KafkaTemplate<String, Object> template) {
        return kafkaRetryPropertiesBuilder()
                .create(template);
    }

    @Bean
    @ConfigurationProperties("spring.kafka.retry.topic")
    RetryTopicProperties.RetryTopicPropertiesBuilder kafkaRetryPropertiesBuilder() {
        return new RetryTopicProperties.RetryTopicPropertiesBuilder();
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

    @Builder(setterPrefix = "set", access = AccessLevel.PACKAGE)
    @lombok.Value
    static class RetryTopicProperties {
        @Builder.Default // spring.kafka.retry.topic.attempts
        Integer attempts = 3;
        @Builder.Default // spring.kafka.retry.topic.auto-create-topics
        Boolean autoCreateTopics = false;
        @Builder.Default // spring.kafka.retry.topic.delay
        Duration delay = Duration.ofSeconds(10);
        @Builder.Default // spring.kafka.retry.topic.dlt-strategy
        DltStrategy dltStrategy = null;
        @Builder.Default // spring.kafka.retry.topic.fixed-delay-strategy
        FixedDelayStrategy fixedDelayStrategy = FixedDelayStrategy.SINGLE_TOPIC;
        @Builder.Default // spring.kafka.retry.topic.max-delay
        Duration maxDelay = null;
        @Builder.Default // spring.kafka.retry.topic.multiplier
        Integer multiplier = null;
        @Builder.Default // spring.kafka.retry.topic.not-retry-on
        List<Class<? extends Throwable>> notRetryOn = null;
        @Builder.Default // spring.kafka.retry.topic.num-partitions
        int numPartitions = -1;
        @Builder.Default // spring.kafka.retry.topic.random-backoff
        boolean randomBackoff = false;
        @Builder.Default // spring.kafka.retry.topic.replication-factor
        short replicationFactor = -1;
        @Builder.Default // spring.kafka.retry.topic.topic-suffix-strategy
        TopicSuffixingStrategy topicSuffixStrategy = null;

        static class RetryTopicPropertiesBuilder {
            private RetryTopicConfiguration create(final KafkaOperations<?, ?> sendToTopicKafkaTemplate) {
                final RetryTopicConfigurationBuilder builder = RetryTopicConfigurationBuilder.newInstance();
                final RetryTopicProperties properties = build();

                setBackoff(properties, builder);
                setAutoCreate(properties, builder);

                setIfNotNull(properties.getAttempts(), builder::maxAttempts);
                setIfNotNull(properties.getDltStrategy(), builder::dltProcessingFailureStrategy);
                setIfNotNull(properties.getFixedDelayStrategy(), builder::useSingleTopicForFixedDelays);
                setIfNotNull(properties.getNotRetryOn(), builder::notRetryOn);
                setIfNotNull(properties.getTopicSuffixStrategy(), builder::setTopicSuffixingStrategy);

                return builder.create(sendToTopicKafkaTemplate);
            }

            private static <T> void setIfNotNull(final T value, final Consumer<T> setter) {
                if (value != null) {
                    setter.accept(value);
                }
            }

            private static void setBackoff(final RetryTopicProperties properties, final RetryTopicConfigurationBuilder builder) {
                if (properties.getDelay() != null) {
                    if (properties.getMaxDelay() == null) {
                        builder.fixedBackOff(properties.getDelay().toMillis());
                    } else if (properties.getMultiplier() == null) {
                        builder.uniformRandomBackoff(properties.getDelay().toMillis(), properties.getMaxDelay().toMillis());
                    } else {
                        builder.exponentialBackoff(properties.getDelay().toMillis(), properties.getMaxDelay().toMillis(), properties.getMultiplier(), properties.isRandomBackoff());
                    }
                }
            }

            private static void setAutoCreate(final RetryTopicProperties properties, final RetryTopicConfigurationBuilder builder) {
                if (properties.getAutoCreateTopics() != null) {
                    if (!properties.getAutoCreateTopics()) {
                        builder.doNotAutoCreateRetryTopics();
                    } else {
                        builder.autoCreateTopicsWith(properties.getNumPartitions(), properties.getReplicationFactor());
                    }
                }
            }
        }
    }
}
