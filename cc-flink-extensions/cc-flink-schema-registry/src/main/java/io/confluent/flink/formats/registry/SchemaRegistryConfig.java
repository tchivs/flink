/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Encapsulates all the configuration required for interaction with Schema Registry. */
@Confluent
public interface SchemaRegistryConfig extends Serializable {

    /** Used it of the schema for the specified table. */
    int getSchemaId();

    /** Client to use for connecting with Schema Registry. */
    SchemaRegistryClient createClient(@Nullable JobID jobID);
}
