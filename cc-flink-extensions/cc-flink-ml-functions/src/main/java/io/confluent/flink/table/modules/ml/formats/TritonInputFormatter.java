/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.utils.mlutils.MlUtils;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Formatter for ML_PREDICT model inputs using NVidia's Triton input format, also known as the <a
 * href="https://github.com/kserve/kserve/blob/master/docs/predict-api/v2/required_api.md">KServe V2
 * format</a>.
 */
public class TritonInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final List<String> tensorTypes;
    private final List<LogicalType> logicalTypes;
    private final Boolean requestBinaryOutput;
    private final Boolean mixedBinaryInput;
    private final DataSerializer.InputSerializer[] inConverters;
    private transient ThreadLocal<Integer> lastJsonLength = new ThreadLocal<>();
    private transient ObjectMapper mapper = new ObjectMapper();

    public TritonInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns,
            List<Schema.UnresolvedColumn> outputColumns) {
        this.inputColumns = inputColumns;
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        tensorTypes = new ArrayList<>(inputColumns.size());
        logicalTypes = new ArrayList<>(inputColumns.size());
        Boolean hasBinaryInput = false;
        for (int i = 0; i < inputColumns.size(); i++) {
            logicalTypes.add(MlUtils.getLogicalType(inputColumns.get(i)));
            try {
                tensorTypes.add(MlUtils.getTritonDataType(logicalTypes.get(i)));
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Unsupported data type for Triton input tensor in ML Predict: "
                                + inputColumns.get(i).getName()
                                + " "
                                + logicalTypes.get(i).toString());
            }
            inConverters[i] = DataSerializer.getSerializer(logicalTypes.get(i));
            LogicalType leafType = MlUtils.getLeafType(logicalTypes.get(i));
            if (leafType.getTypeRoot().equals(LogicalTypeRoot.BINARY)
                    || leafType.getTypeRoot().equals(LogicalTypeRoot.VARBINARY)) {
                hasBinaryInput = true;
            }
        }
        mixedBinaryInput = hasBinaryInput;
        for (int i = 0; i < outputColumns.size(); i++) {
            LogicalType outputType =
                    MlUtils.getLeafType(MlUtils.getLogicalType(outputColumns.get(i)));
            if (outputType.getTypeRoot().equals(LogicalTypeRoot.BINARY)
                    || outputType.getTypeRoot().equals(LogicalTypeRoot.VARBINARY)) {
                requestBinaryOutput = true;
                return;
            }
        }
        requestBinaryOutput = false;
    }

    @Override
    public Map.Entry<String, String>[] headers() {
        if (lastJsonLength.get() == null) {
            return new AbstractMap.SimpleEntry[0];
        }
        return new AbstractMap.SimpleEntry[] {
            new AbstractMap.SimpleEntry<>(
                    "Inference-Header-Content-Length", Integer.toString(lastJsonLength.get()))
        };
    }

    private int getLength(Object array) {
        // The inputs here are generally expected to be Object[], but we also support
        // List and primitive arrays in case non-default external conversions are used.
        if (array instanceof Object[]) {
            return ((Object[]) array).length;
        } else if (array instanceof List) {
            return ((List) array).size();
        } else {
            return Array.getLength(array);
        }
    }

    private Object getFirstElement(Object array) {
        if (array instanceof Object[]) {
            Object[] objectArray = (Object[]) array;
            return objectArray[0];
        } else if (array instanceof List) {
            List<?> list = (List<?>) array;
            return list.get(0);
        } else {
            return Array.get(array, 0);
        }
    }

    private List<Integer> getShapeDimensions(LogicalType type, Object value) {
        // For scalars, the shape is [1], for strings and arrays, the shape is [N], arrays of
        // arrays are [N, M], etc.
        if (type.getTypeRoot().equals(LogicalTypeRoot.ARRAY)) {
            List<Integer> shape = new ArrayList<>();
            shape.add(getLength(value));
            Object subValue = getFirstElement(value);
            if (subValue == null) {
                return Collections.singletonList(0);
            }
            shape.addAll(getShapeDimensions(type.getChildren().get(0), subValue));
            return shape;
        } else if (type.getTypeRoot().equals(LogicalTypeRoot.CHAR)
                || type.getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
            return Collections.singletonList(
                    value.toString().getBytes(StandardCharsets.UTF_8).length);
        } else if (type.getTypeRoot().equals(LogicalTypeRoot.BINARY)
                || type.getTypeRoot().equals(LogicalTypeRoot.VARBINARY)) {
            return Collections.singletonList(((byte[]) value).length);
        }
        // Scalars return an empty list because they don't add a dimension.
        // If the scalar is the only dimension, this should be converted to [1].
        return Collections.emptyList();
    }

    byte[] lengthBytes(byte[] data) {
        byte[] lengthBytes = new byte[4];
        lengthBytes[0] = (byte) (data.length & 0xFF);
        lengthBytes[1] = (byte) ((data.length >> 8) & 0xFF);
        lengthBytes[2] = (byte) ((data.length >> 16) & 0xFF);
        lengthBytes[3] = (byte) ((data.length >> 24) & 0xFF);
        return lengthBytes;
    }

    @Override
    public String contentType() {
        if (mixedBinaryInput) {
            return "application/octet-stream";
        }
        return "application/json";
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
        lastJsonLength.remove();
        final ObjectNode node = mapper.createObjectNode();
        if (requestBinaryOutput) {
            // If any of the output columns are binary, we request binary output for everything.
            // TODO: Do we want to specify this per-output? That might allow for more flexibility
            // for variable length strings, since binary output will force fixed lengths. It also
            // would allow ints/floats to be different sizes in Flink than in Triton.
            final ObjectNode parameters = node.putObject("parameters");
            parameters.put("binary_data_output", true);
        }
        final ArrayNode inputs = node.putArray("inputs");
        // A triton JSON expects "name", "datatype", and "shape" for each input.
        // Data is passed either in "data" or as binary data outside of the JSON.
        // Batching is done by adding a dimension to the shape of each tensor.
        // TODO: Support batching.
        // TODO: We could reuse the same JsonNode on each call for better performance.

        List<byte[]> binaryData = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            final ObjectNode input = inputs.addObject();
            input.put("name", inputColumns.get(i).getName());
            input.put("datatype", tensorTypes.get(i));
            ArrayNode shape = input.putArray("shape");
            for (int dim : getShapeDimensions(logicalTypes.get(i), args[i])) {
                shape.add(dim);
            }
            if (shape.size() == 0) {
                // Scalars get the shape [1].
                shape.add(1);
            }

            LogicalType leafType = MlUtils.getLeafType(logicalTypes.get(i));
            // Binary types get sent as binary data after the JSON.
            if (leafType.getTypeRoot().equals(LogicalTypeRoot.BINARY)
                    || leafType.getTypeRoot().equals(LogicalTypeRoot.VARBINARY)) {
                final ObjectNode parameters = input.putObject("parameters");
                List<byte[]> data = inConverters[i].toBytes(args[i]);
                int totalLength = 0;
                for (byte[] b : data) {
                    byte[] lengthInt = lengthBytes(b);
                    totalLength += b.length + 4;
                    // Put the byte array length as a 4-byte little-endian integer before the data.
                    binaryData.add(lengthInt);
                    binaryData.add(b);
                }
                parameters.put("binary_data_size", totalLength);
            }
            // The data is explicitly NOT required to be converted to a flat array by the spec,
            // so we don't. We do have to make sure not to add an extra array around the data
            // though if it's an array type, so we use putArray or putObject as appropriate.
            else if (logicalTypes.get(i).getTypeRoot().equals(LogicalTypeRoot.ARRAY)) {
                input.put("data", inConverters[i].convert(mapper, null, args[i]));
            } else {
                input.putArray("data").add(inConverters[i].convert(mapper, null, args[i]));
            }
        }
        byte[] json = node.toString().getBytes(StandardCharsets.UTF_8);
        if (binaryData.isEmpty()) {
            return json;
        }
        // For binary data, we need the json length to set the header.
        lastJsonLength.set(json.length);
        byte[] result = new byte[json.length + binaryData.stream().mapToInt(b -> b.length).sum()];
        System.arraycopy(json, 0, result, 0, json.length);
        int offset = json.length;
        for (byte[] b : binaryData) {
            System.arraycopy(b, 0, result, offset, b.length);
            offset += b.length;
        }
        return result;
    }
}
