/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry.utils;

import io.confluent.flink.formats.avro.registry.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * A test {@link SchemaRegistryConfig} that passes the given {@link SchemaRegistryClient}. This lets
 * us connect to a {@link io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry}.
 */
public final class TestSchemaRegistryConfig implements SchemaRegistryConfig {

    private final int schemaId;

    private final SchemaRegistryClient client;

    public TestSchemaRegistryConfig(int schemaId, SchemaRegistryClient client) {
        this.schemaId = schemaId;
        this.client = client;
    }

    @Override
    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public SchemaRegistryClient createClient() {
        return client;
    }
}
