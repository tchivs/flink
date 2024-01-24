/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import io.confluent.flink.runtime.failure.util.FailureMessageUtil;
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
            Stream.of(KEY_TYPE, KEY_MSG).collect(Collectors.toSet());
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

    /* Functional interface to classify a Throwable into a Type. */
    private interface TypeClassifier {
        Optional<Type> classify(Throwable throwable);
    }

    /**
     * Chain of classifiers. We stop on the first match, so the ordering matters. The last one is a
     * catch-all classifier.
     */
    private static final TypeClassifier[] TYPE_CLASSIFIERS = {
        throwable -> {
            // This is meant to capture any exception that has "serializ" in the error message, such
            // as "(de)serialize", "(de)serialization", or "(de)serializable"
            return ExceptionUtils.findThrowableWithMessage(throwable, SERIALIZE_MESSAGE)
                    .map(Throwable::getMessage)
                    .map(
                            msg ->
                                    msg.contains(BROKEN_SERIALIZATION_ERROR_MESSAGE)
                                            ? Type.SYSTEM
                                            : Type.USER);
        },
        forSystemThrowable(TableException.class, Type.USER),
        forSystemThrowable(ArithmeticException.class, Type.USER),
        // Kafka exceptions.
        forUserThrowable(TimeoutException.class, Type.USER),
        forUserThrowable(UnknownTopicOrPartitionException.class, Type.USER),
        // Cast exceptions.
        forSystemThrowable(NumberFormatException.class, Type.USER),
        forSystemThrowable(DateTimeException.class, Type.USER),
        // Authorization exceptions coming from Kafka.
        forUserThrowable(TransactionalIdAuthorizationException.class, Type.USER),
        forUserThrowable(TopicAuthorizationException.class, Type.USER),
        // User Secret error message.
        forPredicate(TypeFailureEnricherUtils::isUserSecretErrorMessage, Type.USER),
        // System exceptions.
        forSystemThrowable(FlinkException.class, Type.SYSTEM),
        forPredicate(ExceptionUtils::isJvmFatalOrOutOfMemoryError, Type.SYSTEM),
        // Catch all.
        throwable -> Optional.of(Type.UNKNOWN)
    };

    /**
     * Helper method to return a Type for a given {@link Throwable} class. Only to be used for all
     * System Throwable classes originating from Flink core and loggers.
     *
     * <p>For everything else, use {@link #forUserThrowable(Class, Type)} as each plugin is loaded
     * through its own classloader. Two class objects are the same only if they are loaded by the
     * same class loader!
     *
     * @param clazz Throwable class
     * @param type Type to return if the Throwable is an instance of the given class
     * @return TypeClassifier
     */
    private static TypeClassifier forSystemThrowable(Class<? extends Throwable> clazz, Type type) {
        return throwable -> ExceptionUtils.findThrowable(throwable, clazz).map(ignored -> type);
    }

    /**
     * Helper method to return a Type for a given {@link Throwable} class. To be used for all User
     * Throwable classes like Connectors, Filesystems etc.
     *
     * <p>TypeFailureEnricher, as every other plugin is loaded through its own classloader. Two
     * class objects are the same only if they are loaded by the same class loader so check by name!
     *
     * @param clazz Throwable class
     * @param type Type to return if the Throwable have the canonical name of the given class
     * @return TypeClassifier
     */
    private static TypeClassifier forUserThrowable(Class<? extends Throwable> clazz, Type type) {
        return throwable ->
                TypeFailureEnricherUtils.findThrowableByName(throwable, clazz).map(ignored -> type);
    }

    private static TypeClassifier forPredicate(Predicate<Throwable> predicate, Type type) {
        return throwable -> {
            if (predicate.test(throwable)) {
                return Optional.of(type);
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
        for (TypeClassifier classifier : TYPE_CLASSIFIERS) {
            final Optional<String> maybeType = classifier.classify(cause).map(Type::name);
            if (maybeType.isPresent()) {
                labels.put(KEY_TYPE, maybeType.get());
                break;
            }
        }
        labels.put(KEY_MSG, FailureMessageUtil.buildMessage(cause));
        LOG.info("Processed failure labels: {}", labels);
        return CompletableFuture.completedFuture(Collections.unmodifiableMap(labels));
    }
}
