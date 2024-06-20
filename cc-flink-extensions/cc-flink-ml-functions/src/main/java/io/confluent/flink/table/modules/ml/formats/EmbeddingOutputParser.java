/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.RemoteRuntimeUtils;

import java.util.List;

/** Parser for embedding output. */
public class EmbeddingOutputParser extends JsonArrayOutputParser {
    public EmbeddingOutputParser(List<Schema.UnresolvedColumn> outputColumns, String modelName) {
        super(outputColumns, getJsonPathFromModelName(modelName));
        // Allow only one output column, which can either be ARRAY<FLOAT> or ARRAY<ARRAY<FLOAT>>.
        if (outputColumns.size() != 1) {
            throw new FlinkRuntimeException(
                    "Default Embedding output format requires a single output column.");
        }
        LogicalType outputType = RemoteRuntimeUtils.getLogicalType(outputColumns.get(0));
        if (!outputType.is(LogicalTypeRoot.ARRAY)) {
            throw new FlinkRuntimeException(
                    "Default Embedding output format requires an ARRAY<FLOAT> or ARRAY<ARRAY<FLOAT>> output column.");
        }
        LogicalType elementType = outputType.getChildren().get(0);
        if (!elementType.is(LogicalTypeRoot.ARRAY) && !elementType.is(LogicalTypeRoot.FLOAT)) {
            throw new FlinkRuntimeException(
                    "Default Embedding output format requires an ARRAY<FLOAT> or ARRAY<ARRAY<FLOAT>> output column.");
        }
        if (elementType.is(LogicalTypeRoot.ARRAY)) {
            LogicalType innerElementType = elementType.getChildren().get(0);
            if (!innerElementType.is(LogicalTypeRoot.FLOAT)) {
                throw new FlinkRuntimeException(
                        "Default Embedding output format requires an ARRAY<FLOAT> or ARRAY<ARRAY<FLOAT>> output column.");
            }
        }
    }

    private static String getJsonPathFromModelName(String modelName) {
        switch (modelName) {
            case "Amazon Titan Embed":
                return "/embedding";
            case "Cohere Embed":
                return "/embeddings";
            case "OpenAI Embed":
                return "/data/0/embedding";
            case "Vertex Embed":
                return "/predictions/0/embeddings/values";
            default:
                throw new IllegalArgumentException(
                        "Unknown model name for embedding output: " + modelName);
        }
    }
}
