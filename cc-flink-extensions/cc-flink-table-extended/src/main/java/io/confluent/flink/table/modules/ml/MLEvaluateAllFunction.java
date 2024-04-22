/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * {@link AggregateFunction} for ML_EVALUATE_ALL, an internal implementation for
 * ML_EVALAUTE(model$all).
 */
public class MLEvaluateAllFunction
        extends AggregateFunction<List<Row>, Map<String, MLEvaluationMetricsAccumulator>> {
    public static final String NAME = "ML_EVALUATE_ALL";
    private final String functionName;
    private final ModelVersions modelVersions;
    // Transient class members which are not serializable.
    private transient Map<String, MLEvaluateFunction> mlEvaluateFunctions;

    public MLEvaluateAllFunction(final String functionName, final ModelVersions modelVersions) {
        this.functionName = functionName;
        this.modelVersions = modelVersions;
        this.mlEvaluateFunctions = new HashMap<>();
        if (modelVersions != null && !modelVersions.getVersions().isEmpty()) {
            for (Map.Entry<String, Tuple2<Boolean, Map<String, String>>> entry :
                    modelVersions.getVersions().entrySet()) {
                String versionId = entry.getKey();
                MLEvaluateFunction mlEvaluateFunction =
                        new MLEvaluateFunction(functionName, entry.getValue().f1);
                mlEvaluateFunctions.put(versionId, mlEvaluateFunction);
            }
        }
    }

    // Return the Accumulators for evaluation metrics of each model version.
    @Override
    public Map<String, MLEvaluationMetricsAccumulator> createAccumulator() {
        Map<String, MLEvaluationMetricsAccumulator> acc = new HashMap<>();
        for (Map.Entry<String, MLEvaluateFunction> entry : mlEvaluateFunctions.entrySet()) {
            acc.put(entry.getKey(), entry.getValue().createAccumulator());
        }
        return acc;
    }

    // Return the input / output / accumulator type inferences based on model tasks.
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        if (mlEvaluateFunctions == null || mlEvaluateFunctions.isEmpty()) {
            throw new IllegalArgumentException("No model versions found for evaluation all.");
        }
        TypeInference basicTypeInference =
                mlEvaluateFunctions.values().iterator().next().getTypeInference(typeFactory);
        if (basicTypeInference == null
                || basicTypeInference.getInputTypeStrategy() == null
                || basicTypeInference.getOutputTypeStrategy() == null) {
            throw new IllegalArgumentException("Type inference not available for " + NAME);
        }
        return TypeInference.newBuilder()
                .inputTypeStrategy(basicTypeInference.getInputTypeStrategy())
                .outputTypeStrategy(
                        callContext -> {
                            List<DataTypes.Field> fields = new ArrayList<>();
                            fields.add(DataTypes.FIELD("VersionId", DataTypes.STRING()));
                            fields.add(DataTypes.FIELD("IsDefaultVersion", DataTypes.BOOLEAN()));
                            DataType rowDataType =
                                    basicTypeInference
                                            .getOutputTypeStrategy()
                                            .inferType(callContext)
                                            .get();

                            RowType rowLogicType = (RowType) rowDataType.getLogicalType();
                            for (int i = 0; i < rowLogicType.getFieldCount(); i++) {
                                fields.add(
                                        DataTypes.FIELD(
                                                rowLogicType.getFieldNames().get(i),
                                                rowDataType.getChildren().get(i)));
                            }
                            return Optional.of(DataTypes.ARRAY(DataTypes.ROW(fields)));
                        })
                .build();
    }

    public void accumulate(Map<String, MLEvaluationMetricsAccumulator> acc, Object... args) {
        for (Map.Entry<String, MLEvaluateFunction> entry : mlEvaluateFunctions.entrySet()) {
            entry.getValue().accumulate(acc.get(entry.getKey()), args);
        }
    }

    public void retract(Map<String, MLEvaluationMetricsAccumulator> acc, Object... args) {
        for (Map.Entry<String, MLEvaluateFunction> entry : mlEvaluateFunctions.entrySet()) {
            entry.getValue().retract(acc.get(entry.getKey()), args);
        }
    }

    public void merge(
            Map<String, MLEvaluationMetricsAccumulator> acc,
            Iterable<Map<String, MLEvaluationMetricsAccumulator>> its) {
        for (Map.Entry<String, MLEvaluateFunction> entry : mlEvaluateFunctions.entrySet()) {
            Iterable<MLEvaluationMetricsAccumulator> result =
                    StreamSupport.stream(its.spliterator(), false)
                            .map(map -> map.get(entry.getKey()))
                            .collect(Collectors.toList());
            entry.getValue().merge(acc.get(entry.getKey()), result);
        }
    }

    @Override
    public List<Row> getValue(Map<String, MLEvaluationMetricsAccumulator> acc) {
        List<Row> result = new ArrayList<>();
        for (Map.Entry<String, MLEvaluateFunction> entry : mlEvaluateFunctions.entrySet()) {
            result.add(
                    Row.of(
                            entry.getKey(),
                            modelVersions.getVersions().get(entry.getKey()).f1,
                            entry.getValue().getValue(acc.get(entry.getKey()))));
        }
        return result;
    }

    public void resetAccumulator(Map<String, MLEvaluationMetricsAccumulator> acc) {
        for (Map.Entry<String, MLEvaluateFunction> entry : mlEvaluateFunctions.entrySet()) {
            entry.getValue().resetAccumulator(acc.get(entry.getKey()));
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        if (mlEvaluateFunctions != null && !mlEvaluateFunctions.isEmpty()) {
            for (Map.Entry<String, MLEvaluateFunction> entry : mlEvaluateFunctions.entrySet()) {
                entry.getValue().open(context);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (mlEvaluateFunctions != null && !mlEvaluateFunctions.isEmpty()) {
            for (Map.Entry<String, MLEvaluateFunction> entry : mlEvaluateFunctions.entrySet()) {
                entry.getValue().close();
            }
        }
        super.close();
    }

    @Override
    public String toString() {
        return String.format("MLEvaluateAllFunction {functionName=%s}", functionName);
    }
}
