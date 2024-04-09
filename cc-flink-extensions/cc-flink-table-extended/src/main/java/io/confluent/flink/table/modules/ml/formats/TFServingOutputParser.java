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

import io.confluent.flink.table.utils.MlUtils;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;

/** Reader for ML_PREDICT TF Serving row format output. */
public class TFServingOutputParser implements OutputParser {
    private final List<Schema.UnresolvedColumn> outputColumns;
    private final DataSerializer.OutputDeserializer[] outConverters;
    private final LogicalType[] dataTypes;
    private final JsonPointer topLevelNode;
    private transient ObjectMapper mapper = new ObjectMapper();

    public TFServingOutputParser(List<Schema.UnresolvedColumn> outputColumns, String topLevelNode) {
        this.outputColumns = outputColumns;
        outConverters = new DataSerializer.OutputDeserializer[outputColumns.size()];
        dataTypes = new LogicalType[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            dataTypes[i] = MlUtils.getLogicalType(outputColumns.get(i));
            outConverters[i] = DataSerializer.getDeserializer(dataTypes[i]);
        }
        if (!topLevelNode.startsWith("/")) {
            topLevelNode = "/" + topLevelNode;
        }
        this.topLevelNode = JsonPointer.compile(topLevelNode);
    }

    public TFServingOutputParser(List<Schema.UnresolvedColumn> outputColumns) {
        this(outputColumns, "/predictions");
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
        // TF Serving row format predictions are returned as an array in the "predictions" field.
        // There is one prediction per input row. Unless we are batching, we will only have one
        // prediction. Models with single outputs don't include the name of the output, but
        // models with multiple outputs include the name of each output in the response.
        final String responseString = MlUtils.getResponseString(response);
        final JsonNode jsonNode;
        try {
            jsonNode = mapper.readTree(responseString);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error parsing ML Predict response: " + e) {};
        }
        final JsonNode predictions = jsonNode.at(topLevelNode);
        if (predictions == null || predictions instanceof MissingNode) {
            // If the response included an error field, we should return that as an exception.
            final JsonNode error = jsonNode.get("error");
            if (error != null) {
                throw new FlinkRuntimeException("Remote ML Predict error: " + error) {};
            }
            throw new FlinkRuntimeException(
                    "No predictions found in ML Predict response: " + responseString) {};
        }
        // The predictions field should be an array of objects, but we only care about the first
        // one since we don't support batching.
        final JsonNode prediction = predictions.get(0);
        // If the prediction has a JSON Object, we have to match the fields of our output schema.
        Row row = Row.withPositions(outputColumns.size());
        if (prediction.isObject()) {
            for (int i = 0; i < outputColumns.size(); i++) {
                final String fieldName = outputColumns.get(i).getName();
                final JsonNode field = prediction.get(fieldName);
                if (field == null) {
                    throw new FlinkRuntimeException(
                            "Field "
                                    + fieldName
                                    + " not found in remote ML Prediction response: "
                                    + prediction) {};
                }

                try {
                    row.setField(i, getRowFieldFromJson(field, i));
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Error deserializing Vertex AI prediction response field "
                                    + fieldName
                                    + ": "
                                    + e.getMessage());
                }
            }
        } else if (outputColumns.size() == 1) {
            // If the prediction is not an object, we assume it's a single column.
            try {
                row.setField(0, getRowFieldFromJson(prediction, 0));
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Error deserializing Vertex AI prediction response: " + e.getMessage());
            }
        } else {
            throw new FlinkRuntimeException(
                    "Remote ML Predict prediction returned a single prediction column "
                            + "when multiple columns were expected") {};
        }

        return row;
    }
}
