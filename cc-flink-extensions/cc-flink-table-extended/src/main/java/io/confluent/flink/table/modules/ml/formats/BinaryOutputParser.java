/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlUtils;
import okhttp3.Response;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/** Parser for ML_PREDICT model outputs. */
public class BinaryOutputParser implements OutputParser {
    private final DataSerializer.OutputDeserializer outConverter;

    public BinaryOutputParser(List<Schema.UnresolvedColumn> outputColumns) {
        // We only allow a single output column for binary output.
        if (outputColumns.size() != 1) {
            throw new FlinkRuntimeException(
                    "ML_PREDICT binary output only supports a single column but got "
                            + outputColumns.size()
                            + " columns");
        }
        LogicalType dataType = MlUtils.getLogicalType(outputColumns.get(0));
        if (dataType.getTypeRoot() == LogicalTypeRoot.ARRAY
                && dataType.getChildren().get(0).getTypeRoot() == LogicalTypeRoot.ARRAY) {
            // We disallow nested arrays.
            throw new FlinkRuntimeException(
                    "Error creating ML Predict response parser: "
                            + "Nested arrays are not supported for binary output.");
        }
        // Row types are not supported.
        if (dataType.getTypeRoot() == LogicalTypeRoot.ROW) {
            throw new FlinkRuntimeException(
                    "Error creating ML Predict response parser: "
                            + "The ROW type is not supported for binary output.");
        }
        // Arrays are only allowed if they contain numeric types or boolean type, the Decimal type
        // is disallowed even though it is a numeric type since it is not fixed length.
        if (dataType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
            if (!dataType.getChildren().get(0).is(LogicalTypeFamily.NUMERIC)
                    && dataType.getChildren().get(0).getTypeRoot() != LogicalTypeRoot.BOOLEAN) {
                throw new FlinkRuntimeException(
                        "Error creating ML Predict response parser: "
                                + "For binary output, array types are only supported if "
                                + "they contain fixed length numeric types or booleans.");
            }
            if (dataType.getChildren().get(0).getTypeRoot() == LogicalTypeRoot.DECIMAL) {
                throw new FlinkRuntimeException(
                        "Error creating ML Predict response parser: "
                                + "For binary output, arrays of DECIMAL types are not supported.");
            }
        }

        outConverter = DataSerializer.getDeserializer(dataType);
    }

    @Override
    public String acceptedContentTypes() {
        return "application/octet-stream";
    }

    @Override
    public Row parse(Response response) throws FlinkRuntimeException {
        byte[] responseBytes = MlUtils.getResponseBytes(response);
        try {
            if (responseBytes.length < outConverter.bytesConsumed()) {
                throw new FlinkRuntimeException(
                        "Error parsing ML Predict response: "
                                + "Response data is too short to contain the expected output");
            }
            ByteBuffer buffer = ByteBuffer.wrap(responseBytes);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            return Row.of(outConverter.convert(buffer, responseBytes.length, null, null));
        } catch (IOException e) {
            throw new FlinkRuntimeException("Error parsing ML Predict response: " + e);
        }
    }
}
