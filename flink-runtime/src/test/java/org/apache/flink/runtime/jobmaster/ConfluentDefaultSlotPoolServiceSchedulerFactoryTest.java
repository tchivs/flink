/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerConfluentOptions;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerFactory;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ConfluentDefaultSlotPoolServiceSchedulerFactoryTest {
    @Test
    void disableWaitTimeout() {
        final Configuration config =
                new Configuration()
                        .set(JobManagerConfluentOptions.ENABLE_RESOURCE_WAIT_TIMEOUT, false);
        AdaptiveSchedulerFactory adaptiveSchedulerFactoryFromConfiguration =
                DefaultSlotPoolServiceSchedulerFactory.getAdaptiveSchedulerFactoryFromConfiguration(
                        config);

        assertThat(adaptiveSchedulerFactoryFromConfiguration.initialResourceAllocationTimeout)
                .isEqualTo(Duration.ofMillis(-1));
    }
}
