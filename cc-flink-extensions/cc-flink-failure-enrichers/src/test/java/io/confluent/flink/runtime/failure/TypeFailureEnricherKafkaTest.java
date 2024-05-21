/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static io.confluent.flink.runtime.failure.TypeFailureEnricherTableITCase.assertFailureEnricherLabels;

/** Unit tests for Kafka Exception classification. */
public class TypeFailureEnricherKafkaTest {

    @Test
    void testSaslAuthenticationException() throws ExecutionException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set(TypeFailureEnricherOptions.ENABLE_JOB_CANNOT_RESTART_LABEL, Boolean.TRUE);

        assertFailureEnricherLabels(
                configuration,
                new SaslAuthenticationException("test_1"),
                "ERROR_CLASS_CODE",
                "17",
                "TYPE",
                "SYSTEM",
                "USER_ERROR_MSG",
                "test_1");

        assertFailureEnricherLabels(
                configuration,
                new SaslAuthenticationException(
                        "test. " + "logicalCluster: CLUSTER_NOT_FOUND" + "!"),
                "ERROR_CLASS_CODE",
                "18",
                "TYPE",
                "USER",
                "USER_ERROR_MSG",
                "test. logicalCluster: CLUSTER_NOT_FOUND!");

        // Check that we also match nested exceptions
        assertFailureEnricherLabels(
                configuration,
                new FlinkRuntimeException(
                        "test root",
                        new SaslAuthenticationException(
                                "test. " + "logicalCluster: CLUSTER_NOT_FOUND" + "!")),
                "ERROR_CLASS_CODE",
                "18",
                "TYPE",
                "USER",
                "USER_ERROR_MSG",
                "test root\nCaused by: test. logicalCluster: CLUSTER_NOT_FOUND!");
    }
}
