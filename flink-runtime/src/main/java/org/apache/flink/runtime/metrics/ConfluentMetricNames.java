/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.metrics;

import org.apache.flink.annotation.Confluent;

/** Collection of internal metric names. */
@Confluent
public class ConfluentMetricNames {
    ConfluentMetricNames() {}

    public static final String NUM_RESCALE_RESTARTS = "numRescaleRestarts";
    public static final String NUM_ERROR_RESTARTS = "numErrorRestarts";
}
