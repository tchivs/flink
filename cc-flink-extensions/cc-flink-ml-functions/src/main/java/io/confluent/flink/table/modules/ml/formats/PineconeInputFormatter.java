/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlUtils;

import java.util.List;

/** Input formatter for pinecone. */
public class PineconeInputFormatter extends JsonObjectInputFormatter {
    public PineconeInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns, TextGenerationParams params) {
        super(getPineconeInputColumns(inputColumns), overrideValuesParam(params));
    }

    static List<Schema.UnresolvedColumn> getPineconeInputColumns(
            List<Schema.UnresolvedColumn> inputColumns) {
        // Pinecone input is a vector of numerics named "vector", and an integer named "topK".
        // The datatypes should have already been validated by the function, but we'll check them
        // here for safety, then just pass them through with the correct names.
        if (inputColumns.size() != 2) {
            throw new FlinkRuntimeException(
                    "Pinecone input must have exactly two columns: 'topK' and 'vector'.");
        }
        LogicalType topKType = MlUtils.getLogicalType(inputColumns.get(0));
        if (!topKType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.INTEGER_NUMERIC)) {
            throw new FlinkRuntimeException(
                    "Pinecone topK input must be an integer, got " + topKType);
        }
        LogicalType vectorType = MlUtils.getLogicalType(inputColumns.get(1));
        if (vectorType.getTypeRoot() != LogicalTypeRoot.ARRAY) {
            throw new FlinkRuntimeException(
                    "Pinecone vector input must be an array, got " + vectorType);
        }
        List<Schema.UnresolvedColumn> unresolvedColumns = new java.util.ArrayList<>();
        unresolvedColumns.add(
                new Schema.UnresolvedPhysicalColumn(
                        "topK", MlUtils.getAbstractType(inputColumns.get(0))));
        unresolvedColumns.add(
                new Schema.UnresolvedPhysicalColumn(
                        "vector", MlUtils.getAbstractType(inputColumns.get(1))));
        return unresolvedColumns;
    }

    static TextGenerationParams overrideValuesParam(TextGenerationParams params) {
        // We need the "includeValues" and "includeMetadata" parameter to be true to get the output
        // vectors and columns, so we set it regardless of the user's input.
        params.overrideParam("includeValues", Boolean.TRUE);
        params.overrideParam("includeMetadata", Boolean.TRUE);
        return params;
    }
}
