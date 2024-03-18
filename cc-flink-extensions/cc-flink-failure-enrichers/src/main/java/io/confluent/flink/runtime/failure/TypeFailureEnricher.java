/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.runtime.failure.util.FailureMessageUtil;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Type implementation of {@link FailureEnricher} that aims to categorize failures to USER or SYSTEM
 * based on the class of the failure.
 */
public class TypeFailureEnricher implements FailureEnricher {

    private static final Logger LOG = LoggerFactory.getLogger(TypeFailureEnricher.class);

    private static final String KEY_TYPE = "TYPE";
    private static final String KEY_MSG = "USER_ERROR_MSG";
    private static final Set<String> ALLOWED_KEYS =
            Stream.of(KEY_TYPE, KEY_MSG, FailureEnricher.KEY_JOB_CANNOT_RESTART)
                    .collect(Collectors.toSet());
    private static final String SERIALIZE_MESSAGE = "serializ";
    // Copy of org.apache.flink.runtime.io.network.api.serialization.NonSpanningWrapper;
    private static final String BROKEN_SERIALIZATION_ERROR_MESSAGE =
            "Serializer consumed more bytes than the record had. "
                    + "This indicates broken serialization. If you are using custom serialization types "
                    + "(Value or Writable), check their serialization methods. If you are using a ";

    /* Enum to map a Throwable into a Type. */
    private enum Type {
        USER,
        SYSTEM,
        UNKNOWN
    }

    private enum Handling {
        FAIL,
        RECOVER,
    }

    /* Functional interface to classify a Throwable into a Type. */
    private interface Classifier {
        Optional<Classification> classify(Throwable throwable);
    }

    /**
     * Chain of classifiers. We stop on the first match, so the ordering matters. The last one is a
     * catch-all classifier.
     */
    private static final Classifier[] TYPE_CLASSIFIERS = {
        forThrowableByMessage(
                // This is meant to capture any exception that has "serializ" in the error message,
                // such as "(de)serialize", "(de)serialization", or "(de)serializable"
                SERIALIZE_MESSAGE,
                Classification.of(Type.USER, Handling.FAIL),
                ConditionalClassification.of(
                        t ->
                                t.getMessage() != null
                                        && t.getMessage()
                                                .contains(BROKEN_SERIALIZATION_ERROR_MESSAGE),
                        Type.SYSTEM,
                        Handling.RECOVER)),
        forSystemThrowable(
                TableException.class,
                Classification.of(Type.USER, Handling.RECOVER),
                ConditionalClassification.of(
                        t ->
                                t.getMessage() != null
                                        && t.getMessage()
                                                .contains("a null value is being written into it"),
                        Type.USER,
                        Handling.FAIL)),
        forSystemThrowable(ArithmeticException.class, Classification.of(Type.USER, Handling.FAIL)),
        // Kafka exceptions.
        forUserThrowable(
                TimeoutException.class,
                Classification.of(Type.USER, Handling.RECOVER),
                ConditionalClassification.of(
                        t ->
                                t.getMessage() != null
                                        && t.getMessage()
                                                .matches(".*Topic .* not present in metadata.*"),
                        Type.USER,
                        Handling.FAIL)),
        forUserThrowable(
                UnknownTopicOrPartitionException.class,
                Classification.of(Type.USER, Handling.FAIL)),
        // Schema Registry exceptions.
        forUserThrowable(RestClientException.class, Classification.of(Type.USER, Handling.RECOVER)),
        // Cast exceptions.
        forSystemThrowable(
                NumberFormatException.class, Classification.of(Type.USER, Handling.RECOVER)),
        forSystemThrowable(DateTimeException.class, Classification.of(Type.USER, Handling.RECOVER)),
        // Authorization exceptions coming from Kafka.
        forUserThrowable(
                TransactionalIdAuthorizationException.class,
                Classification.of(Type.USER, Handling.RECOVER)),
        forUserThrowable(
                TopicAuthorizationException.class, Classification.of(Type.USER, Handling.RECOVER)),
        // User Secret error message.
        forPredicate(
                TypeFailureEnricherUtils::isUserSecretErrorMessage,
                Classification.of(Type.USER, Handling.RECOVER)),
        // System exceptions.
        forSystemThrowable(FlinkException.class, Classification.of(Type.SYSTEM, Handling.RECOVER)),
        forPredicate(
                ExceptionUtils::isJvmFatalOrOutOfMemoryError,
                Classification.of(Type.SYSTEM, Handling.RECOVER)),
        // Catch all.
        throwable -> Optional.of(Classification.of(Type.UNKNOWN, Handling.RECOVER))
    };

    /**
     * Helper method to return a Type for a given {@link Throwable} class. Only to be used for all
     * System Throwable classes originating from Flink core and loggers.
     *
     * <p>For everything else, use {@link #forUserThrowable(Class, Classification,
     * ConditionalClassification...)} as each plugin is loaded through its own classloader. Two
     * class objects are the same only if they are loaded by the same class loader!
     *
     * @param clazz Throwable class
     * @param classification Classification to return if no conditional classification option
     *     applies
     * @param conditionalClassifications Conditional classifications that are checked in order, and
     *     the first match is returned
     * @return TypeClassifier
     */
    private static Classifier forSystemThrowable(
            Class<? extends Throwable> clazz,
            Classification classification,
            ConditionalClassification... conditionalClassifications) {
        return throwable ->
                findSystemThrowable(throwable, clazz)
                        .map(
                                matchedThrowable ->
                                        classifyMatchedThrowable(
                                                matchedThrowable,
                                                classification,
                                                conditionalClassifications));
    }

    /**
     * Helper method to return a Type for a given {@link Throwable} class. To be used for all User
     * Throwable classes like Connectors, Filesystems etc.
     *
     * <p>TypeFailureEnricher, as every other plugin is loaded through its own classloader. Two
     * class objects are the same only if they are loaded by the same class loader so check by name!
     *
     * @param clazz Throwable class
     * @param classification Classification to return if no conditional classification option
     *     applies
     * @param conditionalClassifications Conditional classifications that are checked in order, and
     *     the first match is returned
     * @return TypeClassifier
     */
    private static Classifier forUserThrowable(
            Class<? extends Throwable> clazz,
            Classification classification,
            ConditionalClassification... conditionalClassifications) {
        return throwable ->
                findUserThrowable(throwable, clazz)
                        .map(
                                matchedThrowable ->
                                        classifyMatchedThrowable(
                                                matchedThrowable,
                                                classification,
                                                conditionalClassifications));
    }

    /**
     * Helper method to return a Type for a given {@link Throwable} class. Useful to match Throwable
     * instances by their message.
     *
     * @param message Message to match against the messages in the Throwable chain
     * @param classification Classification to return if no conditional classification option
     *     applies
     * @param conditionalClassifications Conditional classifications that are checked in order, and
     *     the first match is returned
     * @return TypeClassifier
     */
    private static Classifier forThrowableByMessage(
            String message,
            Classification classification,
            ConditionalClassification... conditionalClassifications) {
        return throwable ->
                findThrowableByMessage(throwable, message)
                        .map(
                                matchedThrowable ->
                                        classifyMatchedThrowable(
                                                matchedThrowable,
                                                classification,
                                                conditionalClassifications));
    }

    private static <T extends Throwable> Optional<T> findSystemThrowable(
            Throwable topLevel, Class<T> clazz) {
        return ExceptionUtils.findThrowable(topLevel, clazz);
    }

    private static Optional<Throwable> findUserThrowable(
            Throwable topLevel, Class<? extends Throwable> clazz) {
        return TypeFailureEnricherUtils.findThrowableByName(topLevel, clazz);
    }

    private static Optional<Throwable> findThrowableByMessage(Throwable topLevel, String message) {
        return ExceptionUtils.findThrowableWithMessage(topLevel, message);
    }

    private static Classification classifyMatchedThrowable(
            Throwable throwable,
            Classification classification,
            ConditionalClassification... conditionalClassifications) {
        if (conditionalClassifications != null) {
            for (ConditionalClassification conditionalClassification : conditionalClassifications) {
                if (conditionalClassification.checkCondition(throwable)) {
                    return conditionalClassification;
                }
            }
        }
        return classification;
    }

    private static Classifier forPredicate(
            Predicate<Throwable> predicate, Classification classification) {
        return throwable -> {
            if (predicate.test(throwable)) {
                return Optional.of(classification);
            }
            return Optional.empty();
        };
    }

    @Override
    public Set<String> getOutputKeys() {
        return ALLOWED_KEYS;
    }

    @Override
    public CompletableFuture<Map<String, String>> processFailure(
            final Throwable cause, final Context context) {
        LOG.info("Processing failure:", cause);
        if (cause == null) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        final Map<String, String> labels = new HashMap<>();
        for (Classifier classifier : TYPE_CLASSIFIERS) {
            final Optional<Classification> maybeClassification = classifier.classify(cause);
            if (maybeClassification.isPresent()) {
                Classification classification = maybeClassification.get();
                labels.put(KEY_TYPE, classification.type.name());
                if (Handling.FAIL.equals(classification.handling)) {
                    labels.put(FailureEnricher.KEY_JOB_CANNOT_RESTART, "");
                }
                break;
            }
        }
        labels.put(KEY_MSG, FailureMessageUtil.buildMessage(cause));
        LOG.info("Processed failure labels: {}", labels);
        return CompletableFuture.completedFuture(Collections.unmodifiableMap(labels));
    }

    static class Classification {
        final Type type;
        final Handling handling;

        Classification(Type type, Handling handling) {
            this.type = Preconditions.checkNotNull(type);
            this.handling = Preconditions.checkNotNull(handling);
        }

        static Classification of(Type type, Handling handling) {
            return new Classification(type, handling);
        }
    }

    static class ConditionalClassification extends Classification {
        final Predicate<Throwable> condition;

        ConditionalClassification(Predicate<Throwable> condition, Type type, Handling handling) {
            super(type, handling);
            this.condition = condition;
        }

        boolean checkCondition(Throwable throwable) {
            return condition.test(throwable);
        }

        static ConditionalClassification of(
                Predicate<Throwable> condition, Type type, Handling handling) {
            return new ConditionalClassification(condition, type, handling);
        }
    }
}
