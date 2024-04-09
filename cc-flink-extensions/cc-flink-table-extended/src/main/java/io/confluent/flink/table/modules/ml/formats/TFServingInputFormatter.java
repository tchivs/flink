/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.utils.MlUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Formatter for ML_PREDICT model inputs using TF Serving Row input format. */
public class TFServingInputFormatter implements InputFormatter {
    private final List<String> inputNames;
    private final DataSerializer.InputSerializer[] inConverters;
    private final String instanceWrapper;
    private transient ObjectMapper mapper = new ObjectMapper();

    /**
     * Create a new TFServingInputFormatter.
     *
     * @param inputColumns The input columns to the model.
     * @param instanceWrapper The name of the wrapper for the instance. This is used for PyTorch
     *     models, which require the input to be wrapped in a "data" field.
     */
    public TFServingInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns, String instanceWrapper) {
        this.instanceWrapper = instanceWrapper;
        inputNames = new ArrayList<>(inputColumns.size());
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            LogicalType type = MlUtils.getLogicalType(inputColumns.get(i));
            inConverters[i] = createSerializer(type);
            // If the column type is binary, the name must end in "_bytes". We will add it if it
            // isn't there.
            String name = inputColumns.get(i).getName();
            if (type.getTypeRoot() == LogicalTypeRoot.BINARY
                    || type.getTypeRoot() == LogicalTypeRoot.VARBINARY) {
                if (!name.endsWith("_bytes")) {
                    name += "_bytes";
                }
            }
            inputNames.add(name);
        }
    }

    public TFServingInputFormatter(List<Schema.UnresolvedColumn> inputColumns) {
        this(inputColumns, null);
    }

    public static DataSerializer.InputSerializer createSerializer(LogicalType type) {

        if (type.getTypeRoot() == LogicalTypeRoot.BINARY
                || type.getTypeRoot() == LogicalTypeRoot.VARBINARY) {
            final DataSerializer.InputSerializer serializer = DataSerializer.createSerializer(type);

            // For binary types, the TF Serving format requires that the data be wrapped in a "b64"
            // field and b64 encoded. The base serializer will handle the encoding, but we need to
            // add the "b64" field.
            return new DataSerializer.InputSerializer() {
                @Override
                public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                    final ObjectNode node = mapper.createObjectNode();
                    node.put("b64", serializer.convert(mapper, reuse, value));
                    return node;
                }
            };
        } else {
            return DataSerializer.createSerializer(type);
        }
    }

    @Override
    public byte[] format(Object[] args) {
        if (args.length != inputNames.size()) {
            throw new FlinkRuntimeException(
                    "ML Predict argument list didn't match model input columns "
                            + args.length
                            + " != "
                            + inputNames.size());
        }
        final ObjectNode node = mapper.createObjectNode();
        final ArrayNode instances = node.putArray("instances");

        if (args.length == 1) {
            // If we only have one input, it can go directly into an array.
            // PyTorch models have to wrap the data in a "data" field.
            if (instanceWrapper != null) {
                ObjectNode instance = instances.addObject();
                instance.set(instanceWrapper, inConverters[0].convert(mapper, null, args[0]));
            } else {
                instances.add(inConverters[0].convert(mapper, null, args[0]));
            }
        } else {
            // If we have multiple inputs, they need to be tagged in an object.
            // PyTorch models have to wrap the data in a "data" field.
            ObjectNode instance = instances.addObject();
            if (instanceWrapper != null) {
                instance = instance.putObject(instanceWrapper);
            }
            for (int i = 0; i < args.length; i++) {
                instance.put(inputNames.get(i), inConverters[i].convert(mapper, null, args[i]));
            }
        }
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }
}
