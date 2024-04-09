/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import java.util.Map;

/** Formatter for ML_PREDICT model inputs. */
public interface InputFormatter {

    /**
     * Formats the input data to the model.
     *
     * @param args The input data to be formatted.
     * @return The formatted input data.
     */
    byte[] format(Object[] args);

    /** Content type of the formatted input data. */
    default String contentType() {
        // We don't put a charset in our content types because Sagemaker doesn't like it.
        return "application/json";
    }

    /** Additional headers to be sent with the request. */
    default Map.Entry<String, String>[] headers() {
        return new Map.Entry[0];
    }
}
