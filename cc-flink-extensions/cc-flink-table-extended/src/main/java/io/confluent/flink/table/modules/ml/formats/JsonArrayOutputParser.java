/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.flink.table.utils.MlUtils;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;

/** Parser for ML_PREDICT model outputs. */
public class JsonArrayOutputParser implements OutputParser {
    private final List<Schema.UnresolvedColumn> outputColumns;
    private final DataSerializer.OutputDeserializer[] outConverters;
    private final LogicalType[] dataTypes;
    private Boolean isSingleArray = false;
    private int singleArrayNesting = 0;
    private transient ObjectMapper mapper = new ObjectMapper();

    public JsonArrayOutputParser(List<Schema.UnresolvedColumn> outputColumns) {
        this.outputColumns = outputColumns;
        outConverters = new DataSerializer.OutputDeserializer[outputColumns.size()];
        dataTypes = new LogicalType[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            dataTypes[i] = MlUtils.getLogicalType(outputColumns.get(i));
            outConverters[i] = DataSerializer.getDeserializer(dataTypes[i]);
        }
        if (outputColumns.size() == 1 && dataTypes[0].getTypeRoot() == LogicalTypeRoot.ARRAY) {
            isSingleArray = true;
            // Count how many levels of nesting there are in the array.
            singleArrayNesting = 1;
            LogicalType elementType = dataTypes[0].getChildren().get(0);
            while (elementType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
                singleArrayNesting++;
                elementType = elementType.getChildren().get(0);
            }
        }
    }

    private Object getRowFieldFromJson(JsonNode node, int index) throws IOException {
        if (node.isArray() && node.size() == 1 && !dataTypes[index].is(LogicalTypeRoot.ARRAY)) {
            // Take the first element of the array.
            node = node.get(0);
        }
        return outConverters[index].convert(node);
    }

    private Object getSingleArray(JsonNode node) {
        // The json array should have either singleArrayNesting or singleArrayNesting + 1 levels of
        // nesting. If it has singleArrayNesting + 1, we unwrap the outer array.
        int nesting = 0;
        JsonNode nest = node;
        while (nest.isArray() && nest.size() > 0) {
            nesting++;
            nest = nest.get(0);
        }
        try {
            if (nesting == singleArrayNesting) {
                return outConverters[0].convert(node);
            } else if (nesting == singleArrayNesting + 1) {
                return outConverters[0].convert(node.get(0));
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error deserializing ML Prediction response: " + node + ": " + e.getMessage());
        }

        throw new FlinkRuntimeException(
                "Shape of ML Predict response did not match expected output. Expected "
                        + singleArrayNesting
                        + " or "
                        + (singleArrayNesting + 1)
                        + " levels of nesting, but got "
                        + nesting);
    }

    @Override
    public Row parse(Response response) {
        final String responseString = MlUtils.getResponseString(response);
        final JsonNode jsonNode;
        try {
            jsonNode = mapper.readTree(responseString);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to parse ML Predict response as json.") {};
        }
        if (!jsonNode.isArray()) {
            throw new FlinkRuntimeException("ML prediction response was not a JSON array");
        }
        // If there is only one output, and it is an array, we will just use that array.
        // This is likely to be the most common use case.
        if (isSingleArray) {
            return Row.of(getSingleArray(jsonNode));
        }
        if (jsonNode.size() != outConverters.length) {
            throw new FlinkRuntimeException(
                    "Unexpected number of results from ML Predict. Expected "
                            + outConverters.length
                            + " but got "
                            + jsonNode.size());
        }

        Row row = Row.withPositions(outputColumns.size());

        for (int i = 0; i < outputColumns.size(); i++) {
            final JsonNode prediction = jsonNode.get(i);
            try {
                row.setField(i, getRowFieldFromJson(prediction, i));
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Error deserializing ML Prediction response: " + e.getMessage());
            }
        }

        return row;
    }
}
