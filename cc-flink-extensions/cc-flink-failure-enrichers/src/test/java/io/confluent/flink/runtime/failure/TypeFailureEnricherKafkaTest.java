/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.configuration.Configuration;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static io.confluent.flink.runtime.failure.TypeFailureEnricherTableITCase.assertFailureEnricherLabelIsExpectedLabel;

/** Unit tests for Kafka Exception classification. */
public class TypeFailureEnricherKafkaTest {

    @Test
    void testSaslAuthenticationException() throws ExecutionException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set(TypeFailureEnricherOptions.ENABLE_JOB_CANNOT_RESTART_LABEL, Boolean.TRUE);

        assertFailureEnricherLabelIsExpectedLabel(
                configuration,
                new SaslAuthenticationException("test_1"),
                Arrays.asList("ERROR_CLASS_CODE"),
                "SYSTEM",
                "17");

        assertFailureEnricherLabelIsExpectedLabel(
                configuration,
                new SaslAuthenticationException(
                        "test. " + "logicalCluster: CLUSTER_NOT_FOUND" + "!"),
                Arrays.asList("JOB_CANNOT_RESTART", "ERROR_CLASS_CODE"),
                "USER",
                "18");
    }
}
