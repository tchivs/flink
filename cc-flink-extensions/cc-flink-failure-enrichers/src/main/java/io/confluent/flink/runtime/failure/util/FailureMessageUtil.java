/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure.util;

import org.apache.commons.text.WordUtils;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for building user-friendly error messages. Largely adapted from:
 * https://github.com/confluentinc/ksql/blob/master/ksqldb-common/src/main/java/io/confluent/ksql/util/ErrorMessageUtil.java
 */
public final class FailureMessageUtil {

    private static final String PREFIX = "Caused by: ";

    /**
     * Build a failure message containing the message of each throwable in the chain.
     *
     * <p>Throwable messages are separated by new lines.
     *
     * @param throwable the top level error.
     * @return the error message.
     */
    public static String buildMessage(final Throwable throwable) {
        if (throwable == null) {
            return "";
        }

        final List<String> messages = dedup(getMessages(throwable));

        final String msg = messages.remove(0);

        final String causeMsg =
                messages.stream()
                        .filter(s -> !s.isEmpty())
                        .map(cause -> WordUtils.wrap(PREFIX + cause, 80, "\n\t", true))
                        .collect(Collectors.joining(System.lineSeparator()));

        return causeMsg.isEmpty() ? msg : msg + System.lineSeparator() + causeMsg;
    }

    /**
     * Build a list containing the failure message for each throwable in the chain.
     *
     * @param e the top level failure.
     * @return the list of failure messages.
     */
    public static List<String> getMessages(final Throwable e) {
        return getThrowableList(e).stream()
                .map(FailureMessageUtil::getMessage)
                .collect(Collectors.toList());
    }

    private static String getMessage(final Throwable e) {
        if (e instanceof ConnectException) {
            return "Could not connect to the server. "
                    + "Please check the server details are correct and that the server is running.";
        } else {
            final String message = e.getMessage();
            return message == null ? e.toString() : message;
        }
    }

    private static List<Throwable> getThrowableList(final Throwable e) {
        final List<Throwable> list = new ArrayList<>();
        Throwable cause = e;
        while (cause != null && !list.contains(cause)) {
            list.add(cause);
            cause = cause.getCause();
        }
        return list;
    }

    private static List<String> dedup(final List<String> messages) {
        final List<String> dedupedMessages = new ArrayList<>(new LinkedHashSet<>(messages));
        final String message = dedupedMessages.get(0);
        dedupedMessages.subList(1, dedupedMessages.size()).removeIf(message::contains);
        return dedupedMessages;
    }
}
