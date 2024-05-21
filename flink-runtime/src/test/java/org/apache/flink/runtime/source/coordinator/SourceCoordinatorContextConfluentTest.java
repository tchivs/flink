/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Confluent;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests SourceCoordinatorContext. */
@Confluent
public class SourceCoordinatorContextConfluentTest extends SourceCoordinatorTestBase {

    @Test
    void testJobID() throws Exception {
        sourceReady();

        assertThat(context.getJobID()).contains(JOB_ID);
    }
}
