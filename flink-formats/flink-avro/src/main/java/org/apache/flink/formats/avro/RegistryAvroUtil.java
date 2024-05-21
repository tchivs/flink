/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.util.Optional;

/** Contains utility functions for dealing with the avro registry. */
@Confluent
public class RegistryAvroUtil {

    static SchemaCoderProviderContext createContextFrom(
            DeserializationSchema.InitializationContext context) {
        return new SchemaCoderProviderContext() {
            public Optional<JobID> getJobID() {
                return context.getJobID();
            }
        };
    }

    static SchemaCoderProviderContext createContextFrom(
            SerializationSchema.InitializationContext context) {
        return new SchemaCoderProviderContext() {
            public Optional<JobID> getJobID() {
                return context.getJobID();
            }
        };
    }
}
