/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.flink.table.utils.mlutils.MlUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Formatter for ML_PREDICT model inputs for application/x-text input. */
public class TextInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;
    private transient ObjectMapper mapper = new ObjectMapper();

    public TextInputFormatter(List<Schema.UnresolvedColumn> inputColumns) {
        this.inputColumns = inputColumns;
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            // We only accept character types for text input.
            LogicalType logicalType = MlUtils.getLogicalType(inputColumns.get(i));
            LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
            if (typeRoot != LogicalTypeRoot.CHAR && typeRoot != LogicalTypeRoot.VARCHAR) {
                throw new FlinkRuntimeException(
                        "ML_PREDICT text input only accepts STRING, CHAR, or VARCHAR types, but got "
                                + logicalType);
            }
            inConverters[i] = DataSerializer.getSerializer(logicalType);
        }
    }

    @Override
    public String contentType() {
        return "text/plain";
    }

    @Override
    public byte[] format(Object[] args) {
        if (args.length != inputColumns.size()) {
            throw new FlinkRuntimeException(
                    "ML Predict argument list didn't match model input columns "
                            + args.length
                            + " != "
                            + inputColumns.size());
        }
        // Print each arg on it's own line. We expect that the common use case is to have
        // a single string input. Note that each string is allowed to have newlines.
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                text.append("\n");
            }
            // We only allow string types, so they can be directly converted to string.
            text.append(args[i]);
        }
        return text.toString().getBytes(StandardCharsets.UTF_8);
    }
}
