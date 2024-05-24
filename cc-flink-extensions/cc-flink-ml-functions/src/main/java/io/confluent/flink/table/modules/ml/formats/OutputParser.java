/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import okhttp3.Response;

/** Reader for ML_PREDICT model output formats. */
public interface OutputParser {
    /**
     * Formats the input data to the model.
     *
     * @param response The output data returned by the model.
     * @return The deserialized data.
     */
    Row parse(Response response) throws FlinkRuntimeException;

    /** Content types accepted by the parser. */
    default String acceptedContentTypes() {
        // We don't put a charset in our content types because Sagemaker doesn't like it.
        return "application/json";
    }
}
