/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableType;

/** Non-recoverable exception that fails a job. */
@Confluent
@ThrowableAnnotation(ThrowableType.NonRecoverableError)
public class ConfluentFailException extends Exception {
    public ConfluentFailException(String reason) {
        super(reason);
    }
}
