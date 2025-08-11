package org.watson.demos.utilities;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class KafkaExceptionUtility {
    private static final Predicate<String> allClass = s -> true;
    private static final Predicate<String> nonSpringKafkaClass = s -> !s.startsWith("org.springframework.kafka");
    private static final Pattern exceptionClassPattern = Pattern.compile("(?:^|\n)(?:Caused by: )?([a-zA-Z0-9.]+Exception)[:\n]");

    static Collection<String> allClasses(final String stack) {
        return findClasses(stack, allClass, 0)
                .collect(Collectors.toUnmodifiableList());
    }

    /** Finds first non-Spring-Kafka exception in the stack */
    static Optional<String> findException(final String stack) {
        return findClasses(stack, nonSpringKafkaClass, 0)
                .findFirst();
    }

    /** Finds second non-Spring-Kafka exception in the stack (aka Cause) */
    static Optional<String> findCause(final String stack) {
        return findClasses(stack, nonSpringKafkaClass, 1)
                .findFirst();
    }

    private static Stream<String> findClasses(final String stack, final Predicate<String> filter, final int skip) {
        return Stream.ofNullable(stack)
                .map(exceptionClassPattern::matcher)
                .flatMap(matcher -> Stream.iterate(matcher, Matcher::find, UnaryOperator.identity())
                        .map(f -> f.group(1)))
                .filter(filter)
                .skip(skip);
    }

    private KafkaExceptionUtility() {}
}
