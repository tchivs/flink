/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.extraction;

import io.confluent.flink.udf.extractor.v1.ErrorCode;

/** Exception representing any non platform error which may happen during extraction. */
public class MetadataExtractionException extends Exception {

    private final ErrorCode code;

    /**
     * Extraction exception.
     *
     * @param code The code representing what went wrong
     * @param message A user visible message we want to return
     * @param cause The underlying error
     */
    public MetadataExtractionException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    /**
     * Extraction exception.
     *
     * @param code The code representing what went wrong
     * @param message A user visible message we want to return
     */
    public MetadataExtractionException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
