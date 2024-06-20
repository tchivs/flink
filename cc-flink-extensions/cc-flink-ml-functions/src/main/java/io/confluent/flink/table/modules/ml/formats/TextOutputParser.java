/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.RemoteRuntimeUtils;
import okhttp3.Response;

import java.util.List;

/** Parser for ML_PREDICT model text outputs. */
public class TextOutputParser implements OutputParser {
    private final Schema.UnresolvedColumn outputColumn;

    public TextOutputParser(List<Schema.UnresolvedColumn> outputColumns) {
        // We only allow a single output column for text output, and it must be a string type.
        if (outputColumns.size() != 1) {
            throw new FlinkRuntimeException(
                    "ML_PREDICT text output only supports a single STRING, CHAR, or VARCHAR type, but got "
                            + outputColumns.size()
                            + " columns");
        }
        this.outputColumn = outputColumns.get(0);
        LogicalType logicalType = RemoteRuntimeUtils.getLogicalType(outputColumn);
        if (logicalType.getTypeRoot() != LogicalTypeRoot.CHAR
                && logicalType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            throw new FlinkRuntimeException(
                    "ML_PREDICT text output only supports a single STRING, CHAR, or VARCHAR type, but got "
                            + logicalType);
        }
    }

    @Override
    public String acceptedContentTypes() {
        return "text/plain";
    }

    @Override
    public Row parse(Response response) {
        final String responseString = RemoteRuntimeUtils.getResponseString(response);
        return Row.of(StringData.fromString(responseString));
    }
}
