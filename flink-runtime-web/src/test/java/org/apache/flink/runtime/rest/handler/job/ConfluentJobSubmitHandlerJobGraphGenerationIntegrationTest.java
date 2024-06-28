/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerConfluentOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.ConfluentJobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.ConfluentJobSubmitRequestBody;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link ConfluentJobSubmitHandler} that ensure the generated JobGraph is actually
 * valid and can be submitted to Flink.
 */
@Confluent
class ConfluentJobSubmitHandlerJobGraphGenerationIntegrationTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(
                                    new Configuration()
                                            .set(
                                                    JobManagerConfluentOptions
                                                            .ENABLE_SUBMISSION_ENDPOINT,
                                                    true))
                            .build());

    @Test
    void testJobGraphSubmission(@InjectClusterClient RestClusterClient<?> client) throws Exception {
        final JobID jobId = JobID.generate();
        final String compiledPlan = ConfluentJobSubmitHandlerTest.loadCompiledPlan();

        submit(client, jobId, null, Collections.singleton(compiledPlan), Collections.emptyMap());

        final JobResult jobResult = client.requestJobResult(jobId).get();
        assertThat(jobResult.getApplicationStatus()).isSameAs(ApplicationStatus.SUCCEEDED);
    }

    private static void submit(
            RestClusterClient<?> client,
            @Nullable JobID jobId,
            @Nullable String savepointPath,
            @Nullable Collection<String> generatorArguments,
            @Nullable Map<String, String> jobConfiguration) {

        client.sendRequest(
                        ConfluentJobSubmitHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        new ConfluentJobSubmitRequestBody(
                                Optional.ofNullable(jobId).map(JobID::toHexString).orElse(null),
                                savepointPath,
                                generatorArguments,
                                jobConfiguration))
                .join();
    }
}
