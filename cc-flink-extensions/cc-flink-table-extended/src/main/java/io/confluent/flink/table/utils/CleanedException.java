/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import static io.confluent.flink.table.utils.ClassifiedException.cleanMessage;

/**
 * Stack of exceptions with cleaned messages. The original exception class is written into the
 * message, the original stack trace is kept.
 */
class CleanedException extends Exception {

    static Exception of(Throwable sensitiveOrigin) {
        if (sensitiveOrigin == null) {
            return null;
        }
        return new CleanedException(sensitiveOrigin);
    }

    private CleanedException(Throwable sensitiveOrigin) {
        super(
                String.format(
                        "%s: %s",
                        sensitiveOrigin.getClass().getName(),
                        cleanMessage(sensitiveOrigin.getMessage())),
                of(sensitiveOrigin.getCause()));
        setStackTrace(sensitiveOrigin.getStackTrace());
    }
}
