/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IllegalUserInputException;
import org.apache.flink.util.IllegalUserInputRuntimeException;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.runtime.failure.util.FailureMessageUtil;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
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
    private static final String KEY_ERROR_CLASS_CODE = "ERROR_CLASS_CODE";
    private static final Set<String> ALLOWED_KEYS =
            Stream.of(
                            KEY_TYPE,
                            KEY_MSG,
                            KEY_ERROR_CLASS_CODE,
                            FailureEnricher.KEY_JOB_CANNOT_RESTART)
                    .collect(Collectors.toSet());
    private static final String SERIALIZE_MESSAGE = "serializ";
    // Copy of org.apache.flink.runtime.io.network.api.serialization.NonSpanningWrapper;
    private static final String BROKEN_SERIALIZATION_ERROR_MESSAGE =
            "Serializer consumed more bytes than the record had. "
                    + "This indicates broken serialization. If you are using custom serialization types "
                    + "(Value or Writable), check their serialization methods. If you are using a ";

    private static final String PEKKO_SERIALIZATION_ERROR =
            "Pekko failed sending the message silently";

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

    private final boolean enableJobCannotRestartTag;

    public TypeFailureEnricher(Configuration conf) {
        enableJobCannotRestartTag =
                conf.getBoolean(TypeFailureEnricherOptions.ENABLE_JOB_CANNOT_RESTART_LABEL);
    }

    /** List of known error classifier codes and descriptions for classification. */
    enum ErrorClass {
        UNKNOWN(0, "Unknown error without a more specific classification."),
        SERIALIZATION_GENERAL(
                1,
                "General serialization error, e.g. a record from the input the could not be deserialized because the record did not match the expected schema."),
        BROKEN_SERIALIZATION(2, "TODO"),
        TABLE_GENERAL(3, "TODO"),
        TABLE_NULL_IN_NOT_NULL(4, "TODO"),
        ARITHMETIC(5, "TODO"),
        KAFKA_TIMEOUT(6, "TODO"),
        KAFKA_TIMEOUT_TOPIC_NOT_PRESENT(7, "TODO"),
        KAFKA_UNKNOWN_PARTITION(8, "TODO"),
        REST_CLIENT(9, "TODO"),
        NUMBER_FORMAT(10, "TODO"),
        DATE_TIME(11, "TODO"),
        KAFKA_TXN_ID_AUTH(12, "TODO"),
        KAFKA_TOPIC_AUTH(13, "TODO"),
        USER_SECRET(14, "TODO"),
        FLINK_GENERAL(15, "TODO"),
        JVM_FATAL_OR_OOM(16, "TODO"),
        KAFKA_SASL_AUTH(17, "TODO"),
        KAFKA_SASL_AUTH_CLUSTER_NOT_FOUND(18, "TODO"),
        PEKKO_SERIALIZATION(19, "TODO"),
        // disabled due to ongoing investigation in FRT-528
        // KAFKA_NOT_AUTHORIZED_TO_ACCESS_GROUP(20, "Kafka group read permissions have been
        // removed."),
        ILLEGAL_USER_INPUT_RUNTIME(
                21,
                "General exception that was caused by illegal input from the user caught at runtime. See exception message for further details."),
        ILLEGAL_USER_INPUT(
                22,
                "General exception that was caused by illegal input from the user. See exception message for further details."),
        ;

        /** Unique code for the error class. */
        final int errorCode;

        /** Description of the error class. Ideally includes an example. */
        final String description;

        ErrorClass(int errorCode, String description) {
            this.errorCode = errorCode;
            this.description = description;
        }

        @Override
        public String toString() {
            return "ErrorCode{"
                    + "errorCode="
                    + errorCode
                    + ", description='"
                    + description
                    + '\''
                    + "} "
                    + super.toString();
        }
    }

    static {
        ErrorClass[] errorClasses = ErrorClass.values();
        Set<Integer> uniqueErrorCodes =
                CollectionUtil.newHashSetWithExpectedSize(errorClasses.length);
        for (ErrorClass value : errorClasses) {
            if (!uniqueErrorCodes.add(value.errorCode)) {
                throw new AssertionError("Error code is not unique: " + value);
            }
        }
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
                Classification.of(Type.USER, Handling.RECOVER, ErrorClass.SERIALIZATION_GENERAL),
                ConditionalClassification.of(
                        t -> containsMessage(t, BROKEN_SERIALIZATION_ERROR_MESSAGE),
                        Type.SYSTEM,
                        Handling.RECOVER,
                        ErrorClass.BROKEN_SERIALIZATION),
                ConditionalClassification.of(
                        t -> containsMessage(t, PEKKO_SERIALIZATION_ERROR),
                        Type.SYSTEM,
                        Handling.RECOVER,
                        ErrorClass.PEKKO_SERIALIZATION)),
        forSystemThrowable(
                TableException.class,
                Classification.of(Type.USER, Handling.RECOVER, ErrorClass.TABLE_GENERAL),
                ConditionalClassification.of(
                        t -> containsMessage(t, "a null value is being written into it"),
                        Type.USER,
                        Handling.RECOVER,
                        ErrorClass.TABLE_NULL_IN_NOT_NULL)),
        forSystemThrowable(
                ArithmeticException.class,
                Classification.of(Type.USER, Handling.FAIL, ErrorClass.ARITHMETIC)),
        // Kafka exceptions.
        forUserThrowable(
                TimeoutException.class,
                Classification.of(Type.USER, Handling.RECOVER, ErrorClass.KAFKA_TIMEOUT),
                ConditionalClassification.of(
                        t ->
                                t.getMessage() != null
                                        && t.getMessage()
                                                .matches(".*Topic .* not present in metadata.*"),
                        Type.USER,
                        Handling.RECOVER,
                        ErrorClass.KAFKA_TIMEOUT_TOPIC_NOT_PRESENT)),
        forUserThrowable(
                UnknownTopicOrPartitionException.class,
                Classification.of(Type.USER, Handling.RECOVER, ErrorClass.KAFKA_UNKNOWN_PARTITION)),
        forUserThrowable(
                SaslAuthenticationException.class,
                Classification.of(Type.SYSTEM, Handling.RECOVER, ErrorClass.KAFKA_SASL_AUTH),
                ConditionalClassification.of(
                        t -> containsMessage(t, "logicalCluster: CLUSTER_NOT_FOUND"),
                        Type.USER,
                        Handling.RECOVER,
                        ErrorClass.KAFKA_SASL_AUTH_CLUSTER_NOT_FOUND)),
        // Schema Registry exceptions.
        forUserThrowable(
                RestClientException.class,
                Classification.of(Type.USER, Handling.RECOVER, ErrorClass.REST_CLIENT)),
        // Cast exceptions.
        forSystemThrowable(
                NumberFormatException.class,
                Classification.of(Type.USER, Handling.FAIL, ErrorClass.NUMBER_FORMAT)),
        forSystemThrowable(
                DateTimeException.class,
                Classification.of(Type.USER, Handling.FAIL, ErrorClass.DATE_TIME)),
        // Authorization exceptions coming from Kafka.
        forUserThrowable(
                TransactionalIdAuthorizationException.class,
                Classification.of(Type.USER, Handling.RECOVER, ErrorClass.KAFKA_TXN_ID_AUTH)),
        forUserThrowable(
                TopicAuthorizationException.class,
                Classification.of(Type.USER, Handling.RECOVER, ErrorClass.KAFKA_TOPIC_AUTH)),
        // User Secret error message.
        forPredicate(
                TypeFailureEnricherUtils::isUserSecretErrorMessage,
                Classification.of(Type.USER, Handling.FAIL, ErrorClass.USER_SECRET)),
        // System exceptions.
        forSystemThrowable(
                FlinkException.class,
                Classification.of(Type.SYSTEM, Handling.RECOVER, ErrorClass.FLINK_GENERAL)),
        forPredicate(
                ExceptionUtils::isJvmFatalOrOutOfMemoryError,
                Classification.of(Type.SYSTEM, Handling.RECOVER, ErrorClass.JVM_FATAL_OR_OOM)),
        // General illegal user inputs
        forUserThrowable(
                IllegalUserInputException.class,
                Classification.of(Type.USER, Handling.FAIL, ErrorClass.ILLEGAL_USER_INPUT)),
        forUserThrowable(
                IllegalUserInputRuntimeException.class,
                Classification.of(Type.USER, Handling.FAIL, ErrorClass.ILLEGAL_USER_INPUT_RUNTIME)),
        // Catch all.
        throwable ->
                Optional.of(Classification.of(Type.UNKNOWN, Handling.RECOVER, ErrorClass.UNKNOWN))
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
     * @param fallbackClassification Classification to return if no conditional classification
     *     option applies
     * @param conditionalClassifications Conditional classifications that are checked in order, and
     *     the first match is returned
     * @return TypeClassifier
     */
    private static Classifier forSystemThrowable(
            Class<? extends Throwable> clazz,
            Classification fallbackClassification,
            ConditionalClassification... conditionalClassifications) {
        return throwable ->
                findSystemThrowable(throwable, clazz)
                        .map(
                                matchedThrowable ->
                                        classifyMatchedThrowable(
                                                matchedThrowable,
                                                fallbackClassification,
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
     * @param fallbackClassification Classification to return if no conditional classification
     *     option applies
     * @param conditionalClassifications Conditional classifications that are checked in order, and
     *     the first match is returned
     * @return TypeClassifier
     */
    private static Classifier forUserThrowable(
            Class<? extends Throwable> clazz,
            Classification fallbackClassification,
            ConditionalClassification... conditionalClassifications) {
        return throwable ->
                findUserThrowable(throwable, clazz)
                        .map(
                                matchedThrowable ->
                                        classifyMatchedThrowable(
                                                matchedThrowable,
                                                fallbackClassification,
                                                conditionalClassifications));
    }

    /**
     * Helper method to return a Type for a given {@link Throwable} class. Useful to match Throwable
     * instances by their message.
     *
     * @param message Message to match against the messages in the Throwable chain
     * @param fallbackClassification Classification to return if no conditional classification
     *     option applies
     * @param conditionalClassifications Conditional classifications that are checked in order, and
     *     the first match is returned
     * @return TypeClassifier
     */
    private static Classifier forThrowableByMessage(
            String message,
            Classification fallbackClassification,
            ConditionalClassification... conditionalClassifications) {
        return throwable ->
                findThrowableByMessage(throwable, message)
                        .map(
                                matchedThrowable ->
                                        classifyMatchedThrowable(
                                                matchedThrowable,
                                                fallbackClassification,
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

        LOG.info(
                "Processing failure with enableJobCannotRestartTag={} :",
                enableJobCannotRestartTag,
                cause);

        if (cause == null) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        final Map<String, String> labels = new HashMap<>();
        for (Classifier classifier : TYPE_CLASSIFIERS) {
            final Optional<Classification> maybeClassification = classifier.classify(cause);
            if (maybeClassification.isPresent()) {
                Classification classification = maybeClassification.get();
                labels.put(KEY_TYPE, classification.type.name());
                labels.put(KEY_ERROR_CLASS_CODE, classification.errorClassCode);
                if (enableJobCannotRestartTag && Handling.FAIL.equals(classification.handling)) {
                    labels.put(FailureEnricher.KEY_JOB_CANNOT_RESTART, "");
                }
                break;
            }
        }
        labels.put(KEY_MSG, FailureMessageUtil.buildMessage(cause));
        LOG.info("Processed failure labels: {}", labels);
        return CompletableFuture.completedFuture(Collections.unmodifiableMap(labels));
    }

    private static boolean containsMessage(Throwable t, String message) {
        return t.getMessage() != null && t.getMessage().contains(message);
    }

    static class Classification {
        final Type type;
        final Handling handling;
        final String errorClassCode;
        final String description;

        Classification(Type type, Handling handling, String errorClassCode, String description) {
            this.type = Preconditions.checkNotNull(type);
            this.handling = Preconditions.checkNotNull(handling);
            this.errorClassCode = Preconditions.checkNotNull(errorClassCode);
            this.description = Preconditions.checkNotNull(description);
        }

        static Classification of(
                Type type, Handling handling, String errorClassCode, String description) {
            return new Classification(type, handling, errorClassCode, description);
        }

        static Classification of(Type type, Handling handling, ErrorClass codeAndDescription) {
            return of(
                    type,
                    handling,
                    String.valueOf(codeAndDescription.errorCode),
                    codeAndDescription.description);
        }
    }

    static class ConditionalClassification extends Classification {
        final Predicate<Throwable> condition;

        ConditionalClassification(
                Predicate<Throwable> condition,
                Type type,
                Handling handling,
                String errorClassCode,
                String description) {
            super(type, handling, errorClassCode, description);
            this.condition = condition;
        }

        boolean checkCondition(Throwable throwable) {
            return condition.test(throwable);
        }

        static ConditionalClassification of(
                Predicate<Throwable> condition,
                Type type,
                Handling handling,
                String errorClassCode,
                String description) {
            return new ConditionalClassification(
                    condition, type, handling, errorClassCode, description);
        }

        static ConditionalClassification of(
                Predicate<Throwable> condition,
                Type type,
                Handling handling,
                ErrorClass codeAndDescription) {
            return of(
                    condition,
                    type,
                    handling,
                    String.valueOf(codeAndDescription.errorCode),
                    codeAndDescription.description);
        }
    }
}
