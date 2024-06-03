/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.modules.ml.MLFunctionMetrics;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Class implementing VECTOR_SEARCH table function. */
public class VectorSearchFunction extends TableFunction<Object> {
    public static final String NAME = "VECTOR_SEARCH";
    private transient CatalogTable table;
    private final Map<String, String> serializedTableProperties;
    private final Map<String, String> configuration;
    private final String functionName;
    private transient VectorSearchRuntime searchRuntime = null;
    private transient MLFunctionMetrics metrics;

    private static TypeInference getTypeInference(CatalogTable table, DataTypeFactory typeFactory) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        // Vector search input is always topK int and single float array
        final List<DataType> args = new ArrayList<>();
        args.add(DataTypes.INT()); // int type for topK
        // TODO: Pull the embedding column from the table schema and make this match it's type.
        // Any numeric/byte array type should be acceptable here if that's what the input has.
        args.add(DataTypes.ARRAY(DataTypes.FLOAT())); // float array for embedding
        // Output is an array of whole table output, which will contain one row per topK result.
        List<DataTypes.Field> fields =
                table.getUnresolvedSchema().getColumns().stream()
                        .map(
                                unresolvedColumn ->
                                        DataTypes.FIELD(
                                                unresolvedColumn.getName(),
                                                typeFactory.createDataType(
                                                        ((Schema.UnresolvedPhysicalColumn)
                                                                        unresolvedColumn)
                                                                .getDataType())))
                        .collect(Collectors.toList());
        return TypeInference.newBuilder()
                .typedArguments(args)
                .outputTypeStrategy(
                        callContext ->
                                Optional.of(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "search_results",
                                                        DataTypes.ARRAY(DataTypes.ROW(fields))))))
                .build();
    }

    public VectorSearchFunction(
            final String functionName,
            final Map<String, String> serializedTableProperties,
            final Map<String, String> configuration) {
        this.functionName = functionName;
        this.serializedTableProperties = serializedTableProperties;
        this.configuration = configuration;
        if (serializedTableProperties != null && !serializedTableProperties.isEmpty()) {
            table = deserialize(serializedTableProperties);
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return getTypeInference(table, typeFactory);
    }

    public void eval(Object... args) {
        try {
            collect(searchRuntime.run(args));
        } catch (Exception e) {
            throw new FlinkRuntimeException("ML model runtime error:", e);
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.metrics =
                new MLFunctionMetrics(
                        context.getMetricGroup(), MLFunctionMetrics.VECTOR_SEARCH_METRIC_NAME);
        if (searchRuntime != null) {
            searchRuntime.close();
        }
        if (table == null) {
            if (serializedTableProperties == null) {
                throw new FlinkRuntimeException("Table and serializedTable are both null");
            }
            table = deserialize(serializedTableProperties);
        }
        this.searchRuntime =
                VectorSearchRuntime.open(table, configuration, metrics, Clock.systemUTC());
    }

    @Override
    public void close() throws Exception {
        if (searchRuntime != null) {
            searchRuntime.close();
        }
        super.close();
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("VectorSearchFunction{functionName=%s}", functionName);
    }

    public CatalogTable getTable() {
        return table;
    }

    public Map<String, String> getSerializedTableProperties() {
        return serializedTableProperties;
    }

    private static CatalogTable deserialize(final Map<String, String> serializedTableProperties) {
        return CatalogTable.fromProperties(serializedTableProperties);
    }
}
