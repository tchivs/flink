/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlUtils;

import java.util.List;

/** Input formatter for elastic search. */
public class ElasticInputFormatter extends JsonObjectInputFormatter {
    public ElasticInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns, TextGenerationParams params) {
        super(getElasticInputColumns(inputColumns), params);
        // TODO: "field" should be a required in params.
    }

    static List<Schema.UnresolvedColumn> getElasticInputColumns(
            List<Schema.UnresolvedColumn> inputColumns) {
        if (inputColumns.size() != 2) {
            throw new FlinkRuntimeException(
                    "Elastic input must have exactly two columns: 'topK' and 'vector'.");
        }
        LogicalType topKType = MlUtils.getLogicalType(inputColumns.get(0));
        if (!topKType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.INTEGER_NUMERIC)) {
            throw new FlinkRuntimeException(
                    "Elastic topK input must be an integer, got " + topKType);
        }
        LogicalType vectorType = MlUtils.getLogicalType(inputColumns.get(1));
        if (vectorType.getTypeRoot() != LogicalTypeRoot.ARRAY) {
            throw new FlinkRuntimeException(
                    "Elastic vector input must be an array, got " + vectorType);
        }
        List<Schema.UnresolvedColumn> unresolvedColumns = new java.util.ArrayList<>();
        unresolvedColumns.add(
                new Schema.UnresolvedPhysicalColumn(
                        "k", MlUtils.getAbstractType(inputColumns.get(0))));
        unresolvedColumns.add(
                new Schema.UnresolvedPhysicalColumn(
                        "query_vector", MlUtils.getAbstractType(inputColumns.get(1))));
        return unresolvedColumns;
    }
}
