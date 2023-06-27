/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import java.io.Closeable;

/** Versioned interface for integrating with Flink. */
public interface VersionedFlinkIntegration extends Versioned, Closeable {

    default <T extends VersionedFlinkIntegration> T as(Class<T> clazz) {
        @SuppressWarnings("unchecked")
        final T cast = (T) this;
        return cast;
    }
}
