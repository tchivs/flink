/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

/** Shared interface for versioned objects. */
public interface Versioned {

    int getVersion();
}
