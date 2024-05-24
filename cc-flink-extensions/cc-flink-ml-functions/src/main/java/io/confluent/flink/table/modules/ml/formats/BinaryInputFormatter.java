/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/** Formatter for ML_PREDICT model inputs. */
public class BinaryInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;

    public BinaryInputFormatter(List<Schema.UnresolvedColumn> inputColumns) {
        this.inputColumns = inputColumns;
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            LogicalType logicalType = MlUtils.getLogicalType(inputColumns.get(i));
            inConverters[i] = DataSerializer.getSerializer(logicalType);
        }
    }

    @Override
    public String contentType() {
        return "application/octet-stream";
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
        // All the inputs are packed in-order into a single byte array, with conversions done
        // in little-endian order when necessary, as that is the expected byte-order for ONNX
        // and TensorFlow models.
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            for (int i = 0; i < args.length; i++) {
                List<byte[]> inputBytes = inConverters[i].toBytes(args[i]);
                // Short circuit if we only have one input.
                if (args.length == 1 && inputBytes.size() == 1) {
                    return inputBytes.get(0);
                }
                for (byte[] inputByte : inputBytes) {
                    bytes.write(inputByte);
                }
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException("Error converting ML Predict input to byte array", e);
        }
        return bytes.toByteArray();
    }
}
