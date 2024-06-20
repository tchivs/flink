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

import io.confluent.flink.table.utils.RemoteRuntimeUtils;
import okhttp3.MediaType;
import okhttp3.Response;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for ML_PREDICT model outputs using NVidia's Triton output format, also known as the KServe
 * V2 format.
 */
public class TritonOutputParser implements OutputParser {
    private final List<Schema.UnresolvedColumn> outputColumns;
    private final Map<String, Integer> outputColumnIndex;
    private final DataSerializer.OutputDeserializer[] outConverters;
    private final LogicalType[] dataTypes;
    private transient ObjectMapper mapper = new ObjectMapper();

    public TritonOutputParser(List<Schema.UnresolvedColumn> outputColumns) {
        this.outputColumns = outputColumns;
        outConverters = new DataSerializer.OutputDeserializer[outputColumns.size()];
        dataTypes = new LogicalType[outputColumns.size()];
        outputColumnIndex = new HashMap<>();
        for (int i = 0; i < outputColumns.size(); i++) {
            dataTypes[i] = RemoteRuntimeUtils.getLogicalType(outputColumns.get(i));
            // ROW and ARRAY<ROW> types are not supported.
            if (RemoteRuntimeUtils.getLeafType(dataTypes[i]).getTypeRoot() == LogicalTypeRoot.ROW) {
                throw new FlinkRuntimeException(
                        "Error creating ML Predict response parser: "
                                + "For Triton output, the ROW type is not supported.");
            }
            outConverters[i] = DataSerializer.getDeserializer(dataTypes[i]);
            outputColumnIndex.put(outputColumns.get(i).getName(), i);
        }
    }

    @Override
    public String acceptedContentTypes() {
        return "application/json,application/octet-stream,application/vnd.sagemaker-triton.binary+json";
    }

    private List<Integer> jsonNodeToList(JsonNode jsonNode) {
        List<Integer> list = new ArrayList<>();
        if (jsonNode.isArray()) {
            for (final JsonNode objNode : jsonNode) {
                list.add(objNode.asInt());
            }
        }
        return list;
    }

    private Object getRowFieldFromJson(JsonNode node, int index, List<Integer> shapeList)
            throws IOException {
        if (node.isArray() && node.size() == 1 && !dataTypes[index].is(LogicalTypeRoot.ARRAY)) {
            // Take the first element of the array.
            node = node.get(0);
        }
        // If this type is a nested array and the JsonNode is a flat array, use the shape-based
        // deserializer.
        if (dataTypes[index].is(LogicalTypeRoot.ARRAY)
                && dataTypes[index].getChildren().get(0).is(LogicalTypeRoot.ARRAY)
                && node.isArray()
                && !node.isEmpty()
                && !node.get(0).isArray()) {
            return outConverters[index].convert(node, 0, node.size(), shapeList);
        }
        return outConverters[index].convert(node);
    }

    private int getJsonLength(Response response) {
        // If the Inference-Header-Content-Length header is present, use that.
        String header = response.header("Inference-Header-Content-Length");
        if (header != null) {
            return Integer.parseInt(header);
        }
        // If this is a Sagemaker response, check the value in the content type header.
        // application/vnd.sagemaker-triton.binary+json;json-header-size=NUMBER
        MediaType type = response.body().contentType();
        if (type != null && type.parameter("json-header-size") != null) {
            return Integer.parseInt(type.parameter("json-header-size"));
        }
        return (int) response.body().contentLength();
    }

    @Override
    public Row parse(Response response) {
        int jsonLength = getJsonLength(response);
        if (jsonLength == response.body().contentLength()) {
            return parseJson(RemoteRuntimeUtils.getResponseString(response), null, 0);
        }
        byte[] responseBytes = RemoteRuntimeUtils.getResponseBytes(response);
        String responseString = new String(responseBytes, 0, jsonLength, StandardCharsets.UTF_8);
        return parseJson(responseString, responseBytes, jsonLength);
    }

    private Row parseJson(String responseString, byte[] binaryData, int binaryDataOffset) {
        final JsonNode jsonNode;
        try {
            jsonNode = mapper.readTree(responseString);
        } catch (Exception e) {
            // Don't include the underlying error as it may have PII.
            throw new FlinkRuntimeException("Failed to parse ML Predict response as json.") {};
        }

        final JsonNode outputs = jsonNode.get("outputs");
        if (outputs == null) {
            // If the response included an error field, we should return that as an exception.
            final JsonNode error = jsonNode.get("error");
            if (error != null) {
                throw new FlinkRuntimeException("Remote ML Predict error: " + error) {};
            }
            throw new FlinkRuntimeException(
                    "No 'outputs' or 'error' fields found in ML Predict response.") {};
        }

        Row row = Row.withPositions(outputColumns.size());
        // The outputs should be an array of objects, one for each output.
        if (!outputs.isArray()) {
            throw new FlinkRuntimeException(
                    "'outputs' node in ML Predict response was not an array.") {};
        }

        for (int i = 0; i < outputs.size(); i++) {
            final JsonNode output = outputs.get(i);
            if (output == null) {
                continue;
            }
            // Output contains name, shape, datatype, and data fields.
            final String name = output.get("name").asText();
            final int index = outputColumnIndex.getOrDefault(name, -1);
            if (index < 0) {
                // Skip outputs that we don't care about.
                continue;
            }
            final String datatype = output.get("datatype").asText();
            if (datatype == null
                    || !RemoteRuntimeUtils.isAcceptableTritonDataType(dataTypes[index], datatype)) {
                throw new FlinkRuntimeException(
                        "ML Predict found incompatible datatype for output "
                                + name
                                + ": "
                                + datatype
                                + " (expected "
                                + RemoteRuntimeUtils.getTritonDataType(dataTypes[index])
                                + ") to allow Flink type:"
                                + dataTypes[index]);
            }
            // Get the shape.
            final JsonNode shape = output.get("shape");
            if (shape == null || !shape.isArray() || shape.size() == 0) {
                throw new FlinkRuntimeException(
                        "Invalid or missing shape in ML Predict response for output " + name);
            }
            // Check whether the shape is compatible with the output type.
            List<Integer> shapeList = jsonNodeToList(shape);
            if (!RemoteRuntimeUtils.isShapeCompatible(dataTypes[index], shapeList)) {
                throw new FlinkRuntimeException(
                        "ML Predict found incompatible shape for output " + name + ": " + shape);
            }

            final JsonNode data = output.get("data");
            if (data == null) {
                // Data might be binary encoded, check the parameters to be sure.
                final JsonNode parameters = output.get("parameters");
                if (parameters == null || binaryData == null) {
                    throw new FlinkRuntimeException(
                            "No data found in ML Predict response for output " + name);
                }
                final JsonNode binaryDataSize = parameters.get("binary_data_size");
                if (binaryDataSize == null) {
                    throw new FlinkRuntimeException(
                            "No data or binary_data_size found in ML Predict response for output "
                                    + name);
                }
                final int size = binaryDataSize.asInt();
                if (size <= 0 || size > binaryData.length - binaryDataOffset) {
                    throw new FlinkRuntimeException(
                            "Invalid binary data size in ML Predict response for output "
                                    + name
                                    + ": "
                                    + size
                                    + " (expected "
                                    + (binaryData.length - binaryDataOffset)
                                    + ")");
                }
                DataSerializer.ByteReinterpreter reinterpreter =
                        DataSerializer.getByteReinterpreter(datatype);
                ByteBuffer buffer = ByteBuffer.wrap(binaryData, binaryDataOffset, size);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                try {
                    row.setField(
                            index,
                            outConverters[index].convert(buffer, size, shapeList, reinterpreter));
                    binaryDataOffset += size;
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Error deserializing ML Predict response field "
                                    + name
                                    + ": "
                                    + e.getMessage());
                }

            } else {
                // Data is json and might be a nested array or a flat array.
                try {
                    row.setField(index, getRowFieldFromJson(data, index, shapeList));
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Error deserializing ML Predict response field "
                                    + name
                                    + ": "
                                    + e.getMessage());
                }
            }
        }

        return row;
    }
}
