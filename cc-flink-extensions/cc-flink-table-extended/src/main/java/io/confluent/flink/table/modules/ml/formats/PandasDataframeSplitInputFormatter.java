/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.utils.MlUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Formatter for ML_PREDICT model inputs using Pandas DataFrame split input format. */
public class PandasDataframeSplitInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;
    private final String topLevelNode;
    private transient ObjectMapper mapper = new ObjectMapper();

    public PandasDataframeSplitInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns, String topLevelNode) {
        this.inputColumns = inputColumns;
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            inConverters[i] =
                    DataSerializer.getSerializer(MlUtils.getLogicalType(inputColumns.get(i)));
        }
        this.topLevelNode = topLevelNode;
    }

    public PandasDataframeSplitInputFormatter(List<Schema.UnresolvedColumn> inputColumns) {
        this(inputColumns, "dataframe_split");
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
        final ObjectNode node = mapper.createObjectNode();
        final ObjectNode dataframe = node.putObject(topLevelNode);
        // A pandas dataframe JSON expects columns, index, and data fields.

        // The index field is always an array with a single element, a zero, since we are only
        // sending one row.
        dataframe.putArray("index").add(0);
        // The columns field is an array of the column names.
        ArrayNode columns = dataframe.putArray("columns");
        for (Schema.UnresolvedColumn column : inputColumns) {
            columns.add(column.getName());
        }
        // The data field is an array of arrays, where each inner array is a row.
        ArrayNode data = dataframe.putArray("data");
        ArrayNode row = data.addArray();
        for (int i = 0; i < args.length; i++) {
            row.add(inConverters[i].convert(mapper, null, args[i]));
        }
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }
}
