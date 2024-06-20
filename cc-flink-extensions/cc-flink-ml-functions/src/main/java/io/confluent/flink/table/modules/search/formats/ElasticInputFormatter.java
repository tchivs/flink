/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.ml.formats.JsonObjectInputFormatter;
import io.confluent.flink.table.modules.ml.formats.TextGenerationParams;
import io.confluent.flink.table.utils.RemoteRuntimeUtils;

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
        if (inputColumns.size() != 3) {
            throw new FlinkRuntimeException(
                    "Elastic input must have exactly three columns: 'field', 'k' and 'vector'.");
        }
        LogicalType fieldType = RemoteRuntimeUtils.getLogicalType(inputColumns.get(0));
        if (fieldType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            throw new FlinkRuntimeException(
                    "Elastic field input must be a string, got " + fieldType);
        }
        LogicalType kType = RemoteRuntimeUtils.getLogicalType(inputColumns.get(1));
        if (!kType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.INTEGER_NUMERIC)) {
            throw new FlinkRuntimeException("Elastic k input must be an integer, got " + kType);
        }
        LogicalType queryVectorType = RemoteRuntimeUtils.getLogicalType(inputColumns.get(2));
        if (queryVectorType.getTypeRoot() != LogicalTypeRoot.ARRAY) {
            throw new FlinkRuntimeException(
                    "Elastic query_vector input must be an array, got " + queryVectorType);
        }
        List<Schema.UnresolvedColumn> unresolvedColumns = new java.util.ArrayList<>();
        unresolvedColumns.add(
                new Schema.UnresolvedPhysicalColumn(
                        "field", RemoteRuntimeUtils.getAbstractType(inputColumns.get(0))));
        unresolvedColumns.add(
                new Schema.UnresolvedPhysicalColumn(
                        "k", RemoteRuntimeUtils.getAbstractType(inputColumns.get(1))));
        unresolvedColumns.add(
                new Schema.UnresolvedPhysicalColumn(
                        "query_vector", RemoteRuntimeUtils.getAbstractType(inputColumns.get(2))));
        return unresolvedColumns;
    }
}
