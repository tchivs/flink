/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** {@link FlinkClientWrapper} implementation. */
public class FlinkClientWrapperImpl implements FlinkClientWrapper {
    @Override
    public CompletableFuture<?> submitJobGraph(
            JobGraphWrapper jobGraphWrapper, JobManagerLocation jobManagerLocation) {
        final Configuration configuration =
                new Configuration().set(RestOptions.ADDRESS, jobManagerLocation.getHost());

        jobManagerLocation.getPort().ifPresent(port -> configuration.set(RestOptions.PORT, port));

        final ClusterClient<StandaloneClusterId> client;
        try {
            //noinspection resource
            client = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        final CompletableFuture<?> submissionFuture =
                client.submitJob((JobGraph) jobGraphWrapper.unwrapJobGraph());

        submissionFuture.whenCompleteAsync((ign, err) -> client.close());

        return submissionFuture;
    }

    @Override
    public void close() throws IOException {}
}
