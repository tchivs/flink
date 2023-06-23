/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import java.time.DateTimeException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Type implementation of {@link FailureEnricher} that aims to categorize failures to USER or SYSTEM
 * based on the class of the failure.
 */
public class TypeFailureEnricher implements FailureEnricher {

    private static final String typeKey = "TYPE";
    private static final String codeKey = "CODE";
    private static final Set<String> labelKeys =
            Stream.of(typeKey, codeKey).collect(Collectors.toSet());
    private static final String SERIALIZE_MESSAGE = "serializ";
    // Copy of org.apache.flink.runtime.io.network.api.serialization.NonSpanningWrapper;
    private static final String BROKEN_SERIALIZATION_ERROR_MESSAGE =
            "Serializer consumed more bytes than the record had. "
                    + "This indicates broken serialization. If you are using custom serialization types "
                    + "(Value or Writable), check their serialization methods. If you are using a ";

    @Override
    public Set<String> getOutputKeys() {
        return labelKeys;
    }

    @Override
    public CompletableFuture<Map<String, String>> processFailure(
            final Throwable cause, final Context context) {
        return CompletableFuture.supplyAsync(
                () -> {
                    final Map<String, String> labels = new HashMap();
                    if (cause == null) {
                        return labels;
                    }
                    // Base of exception is User/UDF code or Flink platform
                    if (context.getUserClassLoader() != null) {
                        // Class in the top of the stack, from which an exception is thrown, is
                        // loaded from user artifacts in a submitted JAR
                        Optional<Class> classOnStackTop =
                                TypeFailureEnricherUtils.findClassFromStackTraceTop(
                                        cause, context.getUserClassLoader());
                        if (classOnStackTop.isPresent()
                                && TypeFailureEnricherUtils.isUserCodeClassLoader(
                                        classOnStackTop.get().getClassLoader())) {
                            labels.put(codeKey, "USER");
                        } else {
                            labels.put(codeKey, "SYSTEM");
                        }
                    }

                    // This is meant to capture any exception that has "serializ" in the error
                    // message, such as "(de)serialize", "(de)serialization", or "(de)serializable"
                    Optional<Throwable> serializationException =
                            ExceptionUtils.findThrowableWithMessage(cause, SERIALIZE_MESSAGE);
                    if (serializationException.isPresent()) {
                        // check if system error, otherwise it is user error
                        if (serializationException
                                .get()
                                .getMessage()
                                .contains(BROKEN_SERIALIZATION_ERROR_MESSAGE)) {
                            labels.put(typeKey, "SYSTEM");
                        } else {
                            labels.put(typeKey, "USER");
                        }
                    } else if (ExceptionUtils.findThrowable(cause, ArithmeticException.class)
                            .isPresent()) {
                        labels.put(typeKey, "USER");
                    }
                    // Catch cast exceptions
                    else if (ExceptionUtils.findThrowable(cause, NumberFormatException.class)
                                    .isPresent()
                            || ExceptionUtils.findThrowable(cause, DateTimeException.class)
                                    .isPresent()) {
                        labels.put(typeKey, "USER");
                    } else if (ExceptionUtils.findThrowable(cause, FlinkException.class).isPresent()
                            || ExceptionUtils.isJvmFatalOrOutOfMemoryError(cause)) {
                        labels.put(typeKey, "SYSTEM");
                    } else {
                        labels.put(typeKey, "UNKNOWN");
                    }
                    return labels;
                },
                context.getIOExecutor());
    }
}
