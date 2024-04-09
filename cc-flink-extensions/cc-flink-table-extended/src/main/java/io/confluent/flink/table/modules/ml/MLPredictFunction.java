/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Class implementing ML_PREDICT table function. */
public class MLPredictFunction extends TableFunction<Object> {
    public static final String NAME = "ML_PREDICT";
    private transient CatalogModel model;
    private final Map<String, String> serializedModelProperties;
    private final String functionName;
    private transient MLModelRuntime mlModelRuntime = null;

    private static TypeInference getTypeInference(CatalogModel model, DataTypeFactory typeFactory) {
        Preconditions.checkNotNull(model, "Model must not be null.");
        final List<DataType> args = new ArrayList<>();
        // First arg is model name
        args.add(DataTypes.STRING());
        args.addAll(
                model.getInputSchema().getColumns().stream()
                        .map(
                                c ->
                                        typeFactory.createDataType(
                                                ((Schema.UnresolvedPhysicalColumn) c)
                                                        .getDataType()))
                        .collect(Collectors.toList()));
        Preconditions.checkNotNull(args, "Argument data type list of model not set.");
        return TypeInference.newBuilder()
                .typedArguments(args)
                .outputTypeStrategy(
                        callContext ->
                                Optional.of(
                                        DataTypes.ROW(
                                                model.getOutputSchema().getColumns().stream()
                                                        .map(
                                                                unresolvedColumn ->
                                                                        DataTypes.FIELD(
                                                                                unresolvedColumn
                                                                                        .getName(),
                                                                                typeFactory
                                                                                        .createDataType(
                                                                                                ((Schema
                                                                                                                        .UnresolvedPhysicalColumn)
                                                                                                                unresolvedColumn)
                                                                                                        .getDataType())))
                                                        .collect(Collectors.toList()))))
                .build();
    }

    public MLPredictFunction(
            final String functionName, final Map<String, String> serializedModelProperties) {
        this.functionName = functionName;
        this.serializedModelProperties = serializedModelProperties;
        if (serializedModelProperties != null && !serializedModelProperties.isEmpty()) {
            model = deserialize(serializedModelProperties);
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return getTypeInference(model, typeFactory);
    }

    public void eval(Object... args) {
        try {
            collect(mlModelRuntime.run(args));
        } catch (Exception e) {
            throw new FlinkRuntimeException("ML model runtime error:", e);
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        if (mlModelRuntime != null) {
            mlModelRuntime.close();
        }
        if (model == null) {
            if (serializedModelProperties == null) {
                throw new FlinkRuntimeException("Model and serializedModel are both null");
            }
            model = deserialize(serializedModelProperties);
        }
        this.mlModelRuntime = MLModelRuntime.open(model);
    }

    @Override
    public void close() throws Exception {
        if (mlModelRuntime != null) {
            mlModelRuntime.close();
        }
        super.close();
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("MLPredictFunction {functionName=%s}", functionName);
    }

    public CatalogModel getModel() {
        return model;
    }

    public Map<String, String> getSerializedModelProperties() {
        return serializedModelProperties;
    }

    private static CatalogModel deserialize(final Map<String, String> serializedModelProperties) {
        return ResolvedCatalogModel.fromProperties(serializedModelProperties);
    }
}
