/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.RemoteRuntimeUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Formatter for ML_PREDICT model inputs for CSV input. */
public class CSVInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;

    public CSVInputFormatter(List<Schema.UnresolvedColumn> inputColumns) {
        this.inputColumns = inputColumns;
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        // Disallow all nested arrays and non-nested arrays unless it's a single array.
        for (int i = 0; i < inputColumns.size(); i++) {
            if (RemoteRuntimeUtils.getLogicalType(inputColumns.get(i)).getTypeRoot()
                    == LogicalTypeRoot.ARRAY) {
                if (RemoteRuntimeUtils.getLogicalType(inputColumns.get(i))
                                .getChildren()
                                .get(0)
                                .getTypeRoot()
                        == LogicalTypeRoot.ARRAY) {
                    throw new FlinkRuntimeException(
                            "Error creating ML Predict request formatter: "
                                    + "Nested arrays are not supported for CSV input.");
                }
                if (inputColumns.size() > 1) {
                    throw new FlinkRuntimeException(
                            "Error creating ML Predict request formatter: "
                                    + "For CSV input, array types are only supported if "
                                    + "it is the only input.");
                }
            }
            inConverters[i] =
                    DataSerializer.getSerializer(
                            RemoteRuntimeUtils.getLogicalType(inputColumns.get(i)));
        }
    }

    @Override
    public String contentType() {
        return "text/csv";
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
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                csv.append(",");
            }
            csv.append(inConverters[i].toString(args[i]));
        }

        return csv.toString().getBytes(StandardCharsets.UTF_8);
    }
}
