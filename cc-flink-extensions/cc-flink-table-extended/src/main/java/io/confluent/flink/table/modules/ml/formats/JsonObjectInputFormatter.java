/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.utils.mlutils.MlUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Formatter for ML Predict model inputs using plain JSON. */
public class JsonObjectInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;
    private final String objectWrapper;
    private final TextGenerationParams params;
    private final ObjectNode staticNode;
    private transient ObjectMapper mapper = new ObjectMapper();

    public JsonObjectInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns,
            TextGenerationParams params,
            String objectWrapper) {
        this.inputColumns = inputColumns;
        this.objectWrapper = objectWrapper;
        this.params = params;
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            inConverters[i] =
                    DataSerializer.getSerializer(MlUtils.getLogicalType(inputColumns.get(i)));
        }
        staticNode = createStaticJson(params);
    }

    public JsonObjectInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns, TextGenerationParams params) {
        this(inputColumns, params, null);
    }

    ObjectNode createStaticJson(TextGenerationParams params) {
        ObjectNode staticNode = mapper.createObjectNode();
        if (objectWrapper != null) {
            staticNode.putObject(objectWrapper);
        }
        params.linkAllParams(staticNode);
        return staticNode;
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
        final ObjectNode node = staticNode.deepCopy();
        // The objectWrapper node is guaranteed to exist if needed, as we always create it in the
        // constructor.
        final ObjectNode wrap = objectWrapper != null ? (ObjectNode) node.get(objectWrapper) : node;

        for (int i = 0; i < args.length; i++) {
            wrap.set(inputColumns.get(i).getName(), inConverters[i].convert(mapper, null, args[i]));
        }

        return node.toString().getBytes(StandardCharsets.UTF_8);
    }
}
