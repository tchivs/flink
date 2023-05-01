/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests SourceCoordinatorProvider. */
@Confluent
public class SourceCoordinatorProviderConfluentTest {
    private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);
    private static final JobID JOB_ID = new JobID(1234L, 4321L);
    private static final int NUM_SPLITS = 10;

    private SourceCoordinatorProvider<MockSourceSplit> provider;

    @Before
    public void setup() {
        provider =
                new SourceCoordinatorProvider<>(
                        "SourceCoordinatorProviderTest",
                        OPERATOR_ID,
                        new MockSource(Boundedness.BOUNDED, NUM_SPLITS),
                        1,
                        WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                        null,
                        JOB_ID);
    }

    @Test
    public void testJobId() throws Exception {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SPLITS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context);
        SourceCoordinator<?, ?> sourceCoordinator =
                (SourceCoordinator<?, ?>) coordinator.getInternalCoordinator();
        assertTrue(sourceCoordinator.getContext().getJobID().isPresent());
        assertEquals(JOB_ID, sourceCoordinator.getContext().getJobID().get());
    }
}
