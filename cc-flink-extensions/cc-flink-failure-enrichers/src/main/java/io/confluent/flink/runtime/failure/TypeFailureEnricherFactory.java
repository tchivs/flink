/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricherFactory;

/**
 * An interface for creating {@link FailureEnricher}s. There must exist such a factory to be used
 * with this {@link TypeFailureEnricher}.
 */
public class TypeFailureEnricherFactory implements FailureEnricherFactory {

    @Override
    public FailureEnricher createFailureEnricher(Configuration conf) {
        return new TypeFailureEnricher();
    }
}
