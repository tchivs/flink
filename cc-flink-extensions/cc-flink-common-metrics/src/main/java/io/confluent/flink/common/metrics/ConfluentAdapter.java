/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Metadata associated with a given metric, stored alongside it so that it can be provided during
 * reporting time.
 */
@Confluent
public class ConfluentAdapter {
    static final String CURRENT_INPUT_N_WATERMARK_NEW_METRIC_NAME = "currentInputNWatermark";
    static final Pattern CURRENT_INPUT_N_WATERMARK_METRIC_PATTERN =
            Pattern.compile("(currentInput)([0-9]+)(Watermark)");
    static final int INPUT_INDEX_CAPTURING_GROUP = 2;

    static final String SOURCE_PATTERN = "Source:(.*)";
    static final String SINK_PATTERN = "Sink:(.*)";
    static final String WRITER_PATTERN = "(.*): Writer(.*)";
    static final String COMMITTER_PATTERN = "(.*): Committer(.*)";
    static final String INPUT_INDEX_LABEL_KEY = "input_index";
    static final String OPERATOR_NAME = "operator_name";
    static final String TASK_NAME = "task_name";
    static final String OPERATOR_TYPE = "operator_type";
    static final String TASK_TYPE = "task_type";
    /**
     * Possible task/operator types: {@link ConfluentAdapter#SOURCE}, {@link ConfluentAdapter#SINK},
     * {@link ConfluentAdapter#MIDDLE}.
     */
    static final String SOURCE = "source";

    static final String SINK = "sink";
    static final String MIDDLE = "middle";

    /**
     * adaptMetricName makes changes to the metric name if any are required. Currently, we must
     * rename the metric currentInputNWatermark because metricsAPI Doesn't support generic pattern
     * in metric name. A dimension INPUT_INDEX_LABEL_KEY will be added instead.
     */
    public static String adaptMetricName(String metricName) {
        Matcher currentInputNWatermarkMatcher =
                CURRENT_INPUT_N_WATERMARK_METRIC_PATTERN.matcher(metricName);
        if (currentInputNWatermarkMatcher.find()) {
            return currentInputNWatermarkMatcher.replaceFirst(
                    CURRENT_INPUT_N_WATERMARK_NEW_METRIC_NAME);
        }
        return metricName;
    }

    /**
     * getExtraVariables yields extra dimensions we need in flink metrics. Currently we add
     * INPUT_INDEX_LABEL_KEY to indicate input index for currentInputNWatermark, and
     * task/operator_type label to indicate source/middle/sink (for later aggregations)
     */
    public static Map<String, String> adaptVariables(
            String metricName, Map<String, String> variables) {
        Map<String, String> confluentVariables = new HashMap<>();
        confluentVariables.putAll(variables);
        Matcher currentInputNWatermarkMatcher =
                CURRENT_INPUT_N_WATERMARK_METRIC_PATTERN.matcher(metricName);
        if (currentInputNWatermarkMatcher.find()) {
            String inputNIndex = currentInputNWatermarkMatcher.group(INPUT_INDEX_CAPTURING_GROUP);
            confluentVariables.put(INPUT_INDEX_LABEL_KEY, inputNIndex);
        }
        if (variables.containsKey(TASK_NAME)) {
            confluentVariables.put(TASK_TYPE, getTypeByName(variables.get(TASK_NAME)));
        }
        if (variables.containsKey(OPERATOR_NAME)) {
            confluentVariables.put(OPERATOR_TYPE, getTypeByName(variables.get(OPERATOR_NAME)));
        }
        return confluentVariables;
    }

    private static String getTypeByName(String nameLabel) {
        String type = MIDDLE;
        if (nameLabel.matches(SOURCE_PATTERN)) {
            type = SOURCE;
        }
        if (nameLabel.matches(SINK_PATTERN)
                || nameLabel.matches(WRITER_PATTERN)
                || nameLabel.matches(COMMITTER_PATTERN)) {
            type = SINK;
        }
        return type;
    }
}
