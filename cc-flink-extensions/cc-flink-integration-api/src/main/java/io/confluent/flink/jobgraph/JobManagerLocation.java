/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import javax.annotation.Nullable;

import java.util.Optional;

/** Location of a JobManager. */
public class JobManagerLocation {

    private final String host;
    @Nullable private final Integer port;

    public JobManagerLocation(String host) {
        this(host, null);
    }

    public JobManagerLocation(String host, @Nullable Integer port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public Optional<Integer> getPort() {
        return Optional.ofNullable(port);
    }
}
