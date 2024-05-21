/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.MissingNode;

import io.confluent.flink.table.utils.mlutils.MlUtils;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;

/** Parser for ML_PREDICT model outputs. */
public class JsonObjectOutputParser implements OutputParser {
    private final List<Schema.UnresolvedColumn> outputColumns;
    private final DataSerializer.OutputDeserializer[] outConverters;
    private final LogicalType[] dataTypes;
    private final JsonPointer objectWrapper;
    private transient ObjectMapper mapper = new ObjectMapper();

    public JsonObjectOutputParser(
            List<Schema.UnresolvedColumn> outputColumns, String objectWrapper) {
        this.outputColumns = outputColumns;
        outConverters = new DataSerializer.OutputDeserializer[outputColumns.size()];
        dataTypes = new LogicalType[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            dataTypes[i] = MlUtils.getLogicalType(outputColumns.get(i));
            outConverters[i] = DataSerializer.getDeserializer(dataTypes[i]);
        }
        if (objectWrapper != null) {
            if (!objectWrapper.startsWith("/")) {
                this.objectWrapper = JsonPointer.compile("/" + objectWrapper);
            } else {
                this.objectWrapper = JsonPointer.compile(objectWrapper);
            }
        } else {
            this.objectWrapper = null;
        }
    }

    public JsonObjectOutputParser(List<Schema.UnresolvedColumn> outputColumns) {
        this(outputColumns, null);
    }

    private Object getRowFieldFromJson(JsonNode node, int index) throws IOException {
        if (node.isArray() && node.size() == 1 && !dataTypes[index].is(LogicalTypeRoot.ARRAY)) {
            // Take the first element of the array.
            node = node.get(0);
        }
        return outConverters[index].convert(node);
    }

    @Override
    public Row parse(Response response) {
        // TF Serving column format predictions are returned as an array in the "outputs" field,
        // and are really just plain JSON objects. We will parse them as such.
        final String responseString = MlUtils.getResponseString(response);
        final JsonNode jsonNode;
        try {
            jsonNode = mapper.readTree(responseString);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error parsing ML Predict response: response was not valid json") {};
        }
        JsonNode outerNode = jsonNode;

        if (objectWrapper != null) {
            outerNode = jsonNode.at(objectWrapper);
            if (outerNode == null || outerNode instanceof MissingNode) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Error parsing ML Predict response: Expected object field %s not found in json response.",
                                objectWrapper));
            }
        }

        Row row = Row.withPositions(outputColumns.size());
        for (int i = 0; i < outputColumns.size(); i++) {
            Object value = readField(outerNode, i);
            if (value == null) {
                throw new FlinkRuntimeException(
                        "Error deserializing ML Prediction response: "
                                + "Field "
                                + outputColumns.get(i).getName()
                                + " not found in response.");
            }
            row.setField(i, value);
        }
        return row;
    }

    private Object readField(JsonNode outerNode, int i) {
        final String fieldName = outputColumns.get(i).getName();
        final JsonNode field;
        if (outerNode.isArray()) {
            // TF Serving "Column" output format will do this if the output is a single column.
            if (outputColumns.size() != outerNode.size()) {
                throw new FlinkRuntimeException(
                        "Error deserializing ML Prediction response: "
                                + "Expected object field '"
                                + fieldName
                                + "' in response was an array whose size did not "
                                + "match the number of model output columns.");
            }
            field = outerNode.get(i);
        } else if (outerNode.isValueNode() && outputColumns.size() == 1) {
            // For single column outputs, we allow the outer node to point directly to the field.
            field = outerNode;
        } else {
            field = outerNode.get(fieldName);
            if (field == null) {
                // Recurse inside any nested objects, if it doesn't exist in any of them, return
                // null, which will cause the error to be thrown in the calling method.
                for (JsonNode node : outerNode) {
                    if (node.isObject()) {
                        Object value = readField(node, i);
                        if (value != null) {
                            return value;
                        }
                    }
                }
                return null;
            }
        }
        try {
            return getRowFieldFromJson(field, i);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Error deserializing ML Prediction response field: %s: %s",
                            fieldName, e.getMessage()));
        }
    }
}
