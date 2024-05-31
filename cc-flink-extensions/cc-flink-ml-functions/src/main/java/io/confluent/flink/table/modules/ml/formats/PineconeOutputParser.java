/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.MissingNode;

import io.confluent.flink.table.utils.mlutils.MlUtils;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;

/** Output parser for pinecone. */
public class PineconeOutputParser implements OutputParser {
    private final List<Schema.UnresolvedColumn> outputColumns;
    private final DataSerializer.OutputDeserializer[] outConverters;
    private int embeddingColumnIndex = -1;
    private transient ObjectMapper mapper = new ObjectMapper();

    public PineconeOutputParser(
            List<Schema.UnresolvedColumn> outputColumns, String embeddingColumnName) {
        this.outputColumns = outputColumns;
        outConverters = new DataSerializer.OutputDeserializer[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            if (outputColumns.get(i).getName().equals(embeddingColumnName)) {
                embeddingColumnIndex = i;
            }
            outConverters[i] =
                    DataSerializer.getDeserializer(MlUtils.getLogicalType(outputColumns.get(i)));
        }
        if (embeddingColumnIndex == -1) {
            throw new FlinkRuntimeException(
                    "Pinecone output columns did not contain the specified embedding column named '"
                            + embeddingColumnName
                            + "'");
        }
    }

    @Override
    public Row parse(Response response) {
        final String responseString = MlUtils.getResponseString(response);
        final JsonNode jsonNode;
        try {
            jsonNode = mapper.readTree(responseString);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error parsing Pinecone response: response was not valid json") {};
        }
        // Pinecone API reference:
        // https://docs.pinecone.io/reference/api/data-plane/query
        final JsonNode matches = jsonNode.get("matches");
        if (matches == null || matches instanceof MissingNode) {
            throw new FlinkRuntimeException("No 'matches' field found in Pinecone response.");
        }
        if (!matches.isArray()) {
            throw new FlinkRuntimeException("Pinecone 'matches' field is not an array.");
        }

        // matches is an array of objects, each with an id, score, values, and metadata.
        // We take the individual parts from each object create a row from each, then
        // return a single row with an array of those arrays.
        Row[] rowValues = new Row[matches.size()];
        for (int i = 0; i < matches.size(); i++) {
            rowValues[i] = Row.withPositions(outputColumns.size());
            JsonNode match = matches.get(i);
            if (!match.has("values")) {
                throw new FlinkRuntimeException(
                        "Pinecone 'matches' array contained a match without a 'values' field.");
            }
            if (!match.has("metadata")) {
                throw new FlinkRuntimeException(
                        "Pinecone 'matches' array contained a match without a 'metadata' field.");
            }
            // TODO: Do we want the scores, too?
            JsonNode values = match.get("values");
            JsonNode metadata = match.get("metadata");
            try {
                for (int j = 0; j < outputColumns.size(); j++) {
                    if (j == embeddingColumnIndex) {
                        rowValues[i].setField(j, outConverters[j].convert(values));
                    } else {
                        JsonNode col = metadata.get(outputColumns.get(j).getName());
                        if (col == null) {
                            throw new FlinkRuntimeException(
                                    "Pinecone 'metadata' field did not contain a field named '"
                                            + outputColumns.get(j).getName()
                                            + "' for match index "
                                            + i);
                        }
                        rowValues[i].setField(j, outConverters[j].convert(col));
                    }
                }
            } catch (IOException e) {
                throw new FlinkRuntimeException(
                        "Error parsing Pinecone response: " + e.getMessage());
            }
        }

        Row row = Row.of(rowValues);
        return row;
    }
}
