/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.table.utils.ml.ModelOptionsUtils;

import java.util.HashMap;
import java.util.Map;

/** This class encapsulates the aggregation values for ml evaluation metrics. */
public class MLEvaluationMetricsAccumulator {
    public Integer count = 0;
    public RegressionMetricsAccumulator regressionMetricsAccumulator = null;
    public ClassificationMetricsAccumulator classificationMetricsAccumulator = null;
    public ClusteringMetricsAccumulator clusteringMetricsAccumulator = null;
    public TextGenerationMetricsAccumulator textGenerationMetricsAccumulator = null;

    public MLEvaluationMetricsAccumulator(CatalogModel model) {
        if (model == null) {
            throw new IllegalArgumentException("Model must not be null.");
        }

        MLModelCommonConstants.ModelTask modelTask =
                ModelOptionsUtils.getModelTask(model.getOptions());
        if (modelTask.equals(MLModelCommonConstants.ModelTask.CLASSIFICATION)) {
            this.classificationMetricsAccumulator = new ClassificationMetricsAccumulator();
        } else if (modelTask.equals(MLModelCommonConstants.ModelTask.REGRESSION)) {
            this.regressionMetricsAccumulator = new RegressionMetricsAccumulator();
        } else if (modelTask.equals(MLModelCommonConstants.ModelTask.CLUSTERING)) {
            this.clusteringMetricsAccumulator = new ClusteringMetricsAccumulator();
        } else if (modelTask.equals(MLModelCommonConstants.ModelTask.TEXT_GENERATION)) {
            this.textGenerationMetricsAccumulator = new TextGenerationMetricsAccumulator();
        } else if (modelTask.equals(MLModelCommonConstants.ModelTask.EMBEDDING)) {
            throw new IllegalArgumentException(
                    "Evaluation is not currently supported for Embedding models.");
        } else {
            throw new IllegalArgumentException("Model task not supported.");
        }
    }

    /** Aggregation values for regression model evaluation metrics. */
    public static class RegressionMetricsAccumulator {
        public Double totalAbsoluteError = 0.0;
        public Double totalSquaredError = 0.0;
        public Double totalPredictedValue = 0.0;
        public Double totalPredictedValueVariance = 0.0;
    }

    /** Aggregation values for classification model evaluation metrics. */
    public static class ClassificationMetricsAccumulator {
        public Map<String, Map<String, Integer>> confusionMatrix = new HashMap<>();
    }

    /** Aggregation values for clustering model evaluation metrics. */
    public static class ClusteringMetricsAccumulator {
        public Double totalDistanceWithinCluster = 0.0;
        public Double totalDistanceWithNeighbourCluster = 0.0;
    }

    /** Aggregation values for text generation model evaluation metrics. */
    public static class TextGenerationMetricsAccumulator {
        public Double totalSimilarity = 0.0;
    }
}
