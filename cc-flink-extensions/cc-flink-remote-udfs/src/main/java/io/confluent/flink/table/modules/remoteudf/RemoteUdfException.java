/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.util.Preconditions;

/** Exception for problems in communicating with remote UDFs. */
public class RemoteUdfException extends Exception {
    private static final long serialVersionUID = 1L;

    /** Internal error code to categorize problems. This code can determine the retry behavior. */
    private final int errorCode;

    /** Optional payload from the error message. */
    private final byte[] payload;

    public RemoteUdfException(String message, int errorCode, byte[] payload) {
        super(message);
        this.errorCode = errorCode;
        this.payload = Preconditions.checkNotNull(payload);
    }

    @Override
    public String toString() {
        String s = getClass().getName() + ": errorCode " + errorCode;
        String message = getLocalizedMessage();
        return (message != null) ? (s + ": " + message) : s;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public byte[] getPayload() {
        return payload;
    }
}
