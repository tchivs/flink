/*
 * Copyright 2024 Confluent Inc.
 */

package org.apache.flink.kubernetes.utils;

import org.apache.flink.annotation.Confluent;

/** Custom Kubernetes-related constants. */
@Confluent
public class ConfluentConstants {
    private static final String CONFLUENT_LABEL_KEY_PREFIX = "confluent-";

    public static final String CONFLUENT_CLUSTER_ID_LABEL_KEY =
            CONFLUENT_LABEL_KEY_PREFIX + "cluster-id";
    public static final String CONFLUENT_JOB_ID_LABEL_KEY = CONFLUENT_LABEL_KEY_PREFIX + "job-id";
}
