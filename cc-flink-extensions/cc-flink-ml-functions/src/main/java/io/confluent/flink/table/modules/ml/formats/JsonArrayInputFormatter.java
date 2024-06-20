/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import io.confluent.flink.table.utils.RemoteRuntimeUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Formatter for ML Predict model inputs using plain JSON Arrays. Basically fancy CSV. */
public class JsonArrayInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;
    private Boolean isSingleArray = false;
    private transient ObjectMapper mapper = new ObjectMapper();

    public JsonArrayInputFormatter(List<Schema.UnresolvedColumn> inputColumns) {
        this.inputColumns = inputColumns;
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            inConverters[i] =
                    DataSerializer.getSerializer(
                            RemoteRuntimeUtils.getLogicalType(inputColumns.get(i)));
        }
        if (inputColumns.size() == 1
                && RemoteRuntimeUtils.getLogicalType(inputColumns.get(0)).getTypeRoot()
                        == LogicalTypeRoot.ARRAY) {
            isSingleArray = true;
        }
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
        // If there is only one input, and it is an array, we will just use that array.
        // This is likely to be the most common use case.
        if (isSingleArray) {
            return inConverters[0]
                    .convert(mapper, null, args[0])
                    .toString()
                    .getBytes(StandardCharsets.UTF_8);
        }
        final ArrayNode node = mapper.createArrayNode();
        for (int i = 0; i < args.length; i++) {
            node.add(inConverters[i].convert(mapper, null, args[i]));
        }
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }
}
