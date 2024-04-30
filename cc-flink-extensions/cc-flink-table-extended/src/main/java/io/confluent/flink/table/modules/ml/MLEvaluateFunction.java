/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogModel.ModelTask;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Class implementing ML_EVALUATE table aggregation function. */
public class MLEvaluateFunction extends AggregateFunction<Row, MLEvaluationMetricsAccumulator> {
    public static final String NAME = "ML_EVALUATE";
    private transient CatalogModel model;
    private transient ModelTask modelTask;
    private final Map<String, String> serializedModelProperties;
    private final String functionName;
    private transient MLModelRuntime mlModelRuntime = null;
    private transient MLFunctionMetrics metrics = null;

    private static TypeInference getTypeInference(CatalogModel model, DataTypeFactory typeFactory) {
        Preconditions.checkNotNull(model, "Model must not be null.");
        ModelTask modelTask = ModelOptionsUtils.getModelTask(model.getOptions());
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        new InputTypeStrategy() {
                            @Override
                            public ArgumentCount getArgumentCount() {
                                return new ArgumentCount() {
                                    @Override
                                    public boolean isValidCount(int count) {
                                        return count
                                                >= model.getInputSchema().getColumns().size() + 1;
                                    }

                                    @Override
                                    public Optional<Integer> getMinCount() {
                                        // For unsupervised models, the output column is not
                                        // required for evaluation.
                                        return Optional.of(
                                                model.getInputSchema().getColumns().size() + 1);
                                    }

                                    @Override
                                    public Optional<Integer> getMaxCount() {
                                        // For supervised models, the output column is required for
                                        // evaluation.
                                        // Only one output column is supported for now.
                                        return Optional.of(
                                                model.getInputSchema().getColumns().size() + 2);
                                    }
                                };
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                int numArgs = callContext.getArgumentDataTypes().size();
                                if (numArgs < model.getInputSchema().getColumns().size() + 1) {
                                    throw new FlinkRuntimeException(
                                            "ML_EVALUATE must have at least TWO operands. First one is model path and others are input columns from table");
                                }
                                final List<DataType> args = new ArrayList<>();
                                // First arg is model name.
                                args.add(DataTypes.STRING());
                                // Model input columns.
                                args.addAll(
                                        model.getInputSchema().getColumns().stream()
                                                .map(
                                                        c ->
                                                                typeFactory.createDataType(
                                                                        ((Schema
                                                                                                .UnresolvedPhysicalColumn)
                                                                                        c)
                                                                                .getDataType()))
                                                .collect(Collectors.toList()));
                                // Model expected output columns.
                                if (modelTask != CatalogModel.ModelTask.CLUSTERING) {
                                    DataType expectedOutputType =
                                            typeFactory.createDataType(
                                                    ((Schema.UnresolvedPhysicalColumn)
                                                                    model.getOutputSchema()
                                                                            .getColumns()
                                                                            .get(0))
                                                            .getDataType());
                                    if (numArgs != model.getInputSchema().getColumns().size() + 2
                                            && callContext.getArgumentDataTypes().get(numArgs - 1)
                                                    != expectedOutputType) {
                                        throw new FlinkRuntimeException(
                                                "The expected output column (last argument) type does not match for ML_EVALUATE function");
                                    }
                                    args.add(expectedOutputType);
                                }
                                return Optional.of(args);
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                final List<Signature.Argument> arguments = new ArrayList<>();
                                arguments.add(Signature.Argument.of("model"));
                                for (int i = 0;
                                        i < model.getInputSchema().getColumns().size();
                                        i++) {
                                    final Signature.Argument argument =
                                            Signature.Argument.of("input" + i);
                                    arguments.add(argument);
                                }
                                if (modelTask != CatalogModel.ModelTask.CLUSTERING) {
                                    final Signature.Argument argument =
                                            Signature.Argument.of("expectedOutput");
                                    arguments.add(argument);
                                }
                                return Collections.singletonList(Signature.of(arguments));
                            }
                        })
                .outputTypeStrategy(
                        callContext -> {
                            if (modelTask == CatalogModel.ModelTask.CLASSIFICATION) {
                                return Optional.of(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("ACCURACY", DataTypes.DOUBLE()),
                                                DataTypes.FIELD("PRECISION", DataTypes.DOUBLE()),
                                                DataTypes.FIELD("RECALL", DataTypes.DOUBLE()),
                                                DataTypes.FIELD("F1", DataTypes.DOUBLE())));
                            } else if (modelTask == CatalogModel.ModelTask.REGRESSION) {
                                return Optional.of(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "MEAN_ABSOLUTE_ERROR", DataTypes.DOUBLE()),
                                                DataTypes.FIELD(
                                                        "ROOT_MEAN_SQUARED_ERROR",
                                                        DataTypes.DOUBLE())));
                            } else if (modelTask == CatalogModel.ModelTask.CLUSTERING) {
                                return Optional.of(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "SILHOUETTE_COEFFICIENT",
                                                        DataTypes.DOUBLE())));
                            } else if (modelTask == CatalogModel.ModelTask.TEXT_GENERATION) {
                                return Optional.of(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "MEAN_SIMILARITY", DataTypes.DOUBLE())));
                            } else {
                                throw new FlinkRuntimeException(
                                        "unsupported model task type: " + modelTask);
                            }
                        })
                .build();
    }

    public MLEvaluateFunction(
            final String functionName, final Map<String, String> serializedModelProperties) {
        this.functionName = functionName;
        this.serializedModelProperties = serializedModelProperties;
        if (serializedModelProperties != null && !serializedModelProperties.isEmpty()) {
            model = deserialize(serializedModelProperties);
            modelTask = ModelOptionsUtils.getModelTask(model.getOptions());
        }
    }

    @Override
    public MLEvaluationMetricsAccumulator createAccumulator() {
        return new MLEvaluationMetricsAccumulator(model);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return getTypeInference(model, typeFactory);
    }

    public void accumulate(MLEvaluationMetricsAccumulator acc, Object... args) {
        try {
            Row predictResult = mlModelRuntime.run(args);
            try {
                String predictColumnName = model.getOutputSchema().getColumns().get(0).getName();
                Object predictedValue = predictResult.getField(predictColumnName);
                if (modelTask == CatalogModel.ModelTask.CLASSIFICATION) {
                    if (predictedValue == null) {
                        throw new FlinkRuntimeException("The predicted value is null");
                    }
                    acc.count++;
                    String actual = ((String) args[args.length - 1]).toLowerCase();
                    String predicted = ((String) predictedValue).toLowerCase();
                    if (!acc.classificationMetricsAccumulator.confusionMatrix.containsKey(actual)) {
                        acc.classificationMetricsAccumulator.confusionMatrix.put(
                                actual, new HashMap<>());
                    }
                    if (!acc.classificationMetricsAccumulator
                            .confusionMatrix
                            .get(actual)
                            .containsKey(predicted)) {
                        acc.classificationMetricsAccumulator
                                .confusionMatrix
                                .get(actual)
                                .put(predicted, 0);
                    }
                    acc.classificationMetricsAccumulator
                            .confusionMatrix
                            .get(actual)
                            .put(
                                    predicted,
                                    acc.classificationMetricsAccumulator
                                                    .confusionMatrix
                                                    .get(actual)
                                                    .get(predicted)
                                            + 1);
                } else if (modelTask == CatalogModel.ModelTask.REGRESSION) {
                    if (predictedValue == null) {
                        throw new FlinkRuntimeException("The predicted value is null");
                    }
                    acc.count++;
                    double actual = (double) args[args.length - 1];
                    double predicted = (double) predictedValue;
                    acc.regressionMetricsAccumulator.totalAbsoluteError +=
                            Math.abs(actual - predicted);
                    acc.regressionMetricsAccumulator.totalSquaredError +=
                            Math.pow(actual - predicted, 2);
                } else if (modelTask == CatalogModel.ModelTask.CLUSTERING) {
                    acc.count++;
                    // TODO: Matrix to implement clustering evaluation metrics.
                } else if (modelTask == CatalogModel.ModelTask.TEXT_GENERATION) {
                    acc.count++;
                    // TODO: Matrix to implement text generation evaluation metrics.
                } else {
                    throw new FlinkRuntimeException("unsupported model task type: " + modelTask);
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "The output column does not exist in the model output schema");
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("ML model runtime error:", e);
        }
    }

    public void retract(MLEvaluationMetricsAccumulator acc, Object... args) {
        try {
            Row predictResult = mlModelRuntime.run(args);
            try {
                String predictColumnName = model.getOutputSchema().getColumns().get(0).getName();
                Object predictedValue = predictResult.getField(predictColumnName);
                if (modelTask == CatalogModel.ModelTask.CLASSIFICATION) {
                    if (predictedValue == null) {
                        throw new FlinkRuntimeException("The predicted value is null");
                    }
                    acc.count--;
                    String actual = ((String) args[args.length - 1]).toLowerCase();
                    String predicted = ((String) predictedValue).toLowerCase();
                    // It must exist in the confusion matrix.
                    // Add additional check for safety.
                    if (acc.classificationMetricsAccumulator.confusionMatrix.containsKey(actual)
                            && acc.classificationMetricsAccumulator
                                    .confusionMatrix
                                    .get(actual)
                                    .containsKey(predicted)) {

                        acc.classificationMetricsAccumulator
                                .confusionMatrix
                                .get(actual)
                                .put(
                                        predicted,
                                        acc.classificationMetricsAccumulator
                                                        .confusionMatrix
                                                        .get(actual)
                                                        .get(predicted)
                                                - 1);
                    }
                } else if (modelTask == CatalogModel.ModelTask.REGRESSION) {
                    if (predictedValue == null) {
                        throw new FlinkRuntimeException("The predicted value is null");
                    }
                    acc.count--;
                    double actual = (double) args[args.length - 1];
                    double predicted = (double) predictedValue;
                    acc.regressionMetricsAccumulator.totalAbsoluteError -=
                            Math.abs(actual - predicted);
                    acc.regressionMetricsAccumulator.totalSquaredError -=
                            Math.pow(actual - predicted, 2);
                } else if (modelTask == CatalogModel.ModelTask.CLUSTERING) {
                    acc.count--;
                    // TODO: Matrix to implement clustering evaluation metrics.
                } else if (modelTask == CatalogModel.ModelTask.TEXT_GENERATION) {
                    acc.count--;
                    // TODO: Matrix to implement text generation evaluation metrics.
                } else {
                    throw new FlinkRuntimeException("unsupported model task type: " + modelTask);
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "The output column does not exist in the model output schema");
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("ML model runtime error:", e);
        }
    }

    public void merge(
            MLEvaluationMetricsAccumulator acc, Iterable<MLEvaluationMetricsAccumulator> its) {
        if (acc.classificationMetricsAccumulator != null) {
            for (MLEvaluationMetricsAccumulator other : its) {
                acc.count += other.count;
                if (other.classificationMetricsAccumulator != null) {
                    for (Map.Entry<String, Map<String, Integer>> entry :
                            other.classificationMetricsAccumulator.confusionMatrix.entrySet()) {
                        if (!acc.classificationMetricsAccumulator.confusionMatrix.containsKey(
                                entry.getKey())) {
                            acc.classificationMetricsAccumulator.confusionMatrix.put(
                                    entry.getKey(), new HashMap<>());
                        }
                        for (Map.Entry<String, Integer> entry2 : entry.getValue().entrySet()) {
                            if (!acc.classificationMetricsAccumulator
                                    .confusionMatrix
                                    .get(entry.getKey())
                                    .containsKey(entry2.getKey())) {
                                acc.classificationMetricsAccumulator
                                        .confusionMatrix
                                        .get(entry.getKey())
                                        .put(entry2.getKey(), 0);
                            }
                            acc.classificationMetricsAccumulator
                                    .confusionMatrix
                                    .get(entry.getKey())
                                    .put(
                                            entry2.getKey(),
                                            acc.classificationMetricsAccumulator
                                                            .confusionMatrix
                                                            .get(entry.getKey())
                                                            .get(entry2.getKey())
                                                    + entry2.getValue());
                        }
                    }
                }
            }
        } else if (acc.regressionMetricsAccumulator != null) {
            for (MLEvaluationMetricsAccumulator other : its) {
                acc.count += other.count;
                if (other.regressionMetricsAccumulator != null) {
                    acc.regressionMetricsAccumulator.totalAbsoluteError +=
                            other.regressionMetricsAccumulator.totalAbsoluteError;
                    acc.regressionMetricsAccumulator.totalSquaredError +=
                            other.regressionMetricsAccumulator.totalSquaredError;
                }
            }
        } else if (acc.clusteringMetricsAccumulator != null) {
            for (MLEvaluationMetricsAccumulator other : its) {
                acc.count += other.count;
                if (other.clusteringMetricsAccumulator != null) {
                    acc.clusteringMetricsAccumulator.totalDistanceWithinCluster +=
                            other.clusteringMetricsAccumulator.totalDistanceWithinCluster;
                    acc.clusteringMetricsAccumulator.totalDistanceWithNeighbourCluster +=
                            other.clusteringMetricsAccumulator.totalDistanceWithNeighbourCluster;
                }
            }
        } else if (acc.textGenerationMetricsAccumulator != null) {
            for (MLEvaluationMetricsAccumulator other : its) {
                acc.count += other.count;
                if (other.textGenerationMetricsAccumulator != null) {
                    acc.textGenerationMetricsAccumulator.totalSimilarity +=
                            other.textGenerationMetricsAccumulator.totalSimilarity;
                }
            }
        }
    }

    @Override
    public Row getValue(MLEvaluationMetricsAccumulator acc) {
        if (acc.count == 0) {
            return null;
        }
        if (acc.classificationMetricsAccumulator != null) {
            double accuracy = 0.0;
            double precision = 0.0;
            double recall = 0.0;
            double f1 = 0.0;
            for (Map.Entry<String, Map<String, Integer>> entry :
                    acc.classificationMetricsAccumulator.confusionMatrix.entrySet()) {
                int tp = entry.getValue().getOrDefault(entry.getKey(), 0);
                int fp = 0;
                int fn = 0;
                for (Map.Entry<String, Map<String, Integer>> entry2 :
                        acc.classificationMetricsAccumulator.confusionMatrix.entrySet()) {
                    if (!entry2.getKey().equals(entry.getKey())) {
                        fp += entry2.getValue().getOrDefault(entry.getKey(), 0);
                        fn += entry.getValue().getOrDefault(entry2.getKey(), 0);
                    }
                }
                accuracy += (double) tp / acc.count;
                precision += (double) tp / (tp + fp);
                recall += (double) tp / (tp + fn);
                f1 += 2 * precision * recall / (precision + recall);
            }
            return Row.of(
                    accuracy / acc.classificationMetricsAccumulator.confusionMatrix.size(),
                    precision / acc.classificationMetricsAccumulator.confusionMatrix.size(),
                    recall / acc.classificationMetricsAccumulator.confusionMatrix.size(),
                    f1 / acc.classificationMetricsAccumulator.confusionMatrix.size());
        } else if (acc.regressionMetricsAccumulator != null) {
            return Row.of(
                    acc.regressionMetricsAccumulator.totalAbsoluteError / acc.count,
                    Math.sqrt(acc.regressionMetricsAccumulator.totalSquaredError / acc.count));
        } else if (acc.clusteringMetricsAccumulator != null && acc.count > 0) {
            return Row.of(
                    (acc.clusteringMetricsAccumulator.totalDistanceWithNeighbourCluster
                                    - acc.clusteringMetricsAccumulator.totalDistanceWithinCluster)
                            / acc.clusteringMetricsAccumulator.totalDistanceWithNeighbourCluster);
        } else if (acc.textGenerationMetricsAccumulator != null) {
            return Row.of(acc.textGenerationMetricsAccumulator.totalSimilarity / acc.count);
        }
        return null;
    }

    public void resetAccumulator(MLEvaluationMetricsAccumulator acc) {
        acc.count = 0;
        if (acc.classificationMetricsAccumulator != null) {
            acc.classificationMetricsAccumulator.confusionMatrix.clear();
        } else if (acc.regressionMetricsAccumulator != null) {
            acc.regressionMetricsAccumulator.totalAbsoluteError = 0.0;
            acc.regressionMetricsAccumulator.totalSquaredError = 0.0;
        } else if (acc.clusteringMetricsAccumulator != null) {
            acc.clusteringMetricsAccumulator.totalDistanceWithinCluster = 0.0;
            acc.clusteringMetricsAccumulator.totalDistanceWithNeighbourCluster = 0.0;
        } else if (acc.textGenerationMetricsAccumulator != null) {
            acc.textGenerationMetricsAccumulator.totalSimilarity = 0.0;
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.metrics = new MLFunctionMetrics(context.getMetricGroup());
        if (mlModelRuntime != null) {
            mlModelRuntime.close();
        }
        if (model == null) {
            if (serializedModelProperties == null) {
                throw new FlinkRuntimeException("Model and serializedModel are both null");
            }
            model = deserialize(serializedModelProperties);
        }
        this.mlModelRuntime = MLModelRuntime.open(model, metrics, Clock.systemUTC());
    }

    @Override
    public void close() throws Exception {
        if (mlModelRuntime != null) {
            mlModelRuntime.close();
        }
        super.close();
    }

    @Override
    public String toString() {
        return String.format("MLEvaluateFunction {functionName=%s}", functionName);
    }

    public CatalogModel getModel() {
        return model;
    }

    private static CatalogModel deserialize(final Map<String, String> serializedModelProperties) {
        return ResolvedCatalogModel.fromProperties(serializedModelProperties);
    }
}
