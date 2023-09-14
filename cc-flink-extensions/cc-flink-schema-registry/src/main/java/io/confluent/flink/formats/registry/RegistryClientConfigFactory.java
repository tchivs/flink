/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.FormatFactory;

import io.confluent.flink.formats.registry.credentials.DPATCredentialProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Shared across formats factory class for creating a {@link SchemaRegistryConfig}. */
@Confluent
public class RegistryClientConfigFactory {

    /** Creates a {@link SchemaRegistryConfig} from the given format options. */
    public static SchemaRegistryConfig get(ReadableConfig formatOptions) {
        final String schemaRegistryURL = formatOptions.get(RegistryFormatOptions.URL);
        final int schemaId = formatOptions.get(RegistryFormatOptions.SCHEMA_ID);
        final int cacheSize = formatOptions.get(RegistryFormatOptions.SCHEMA_CACHE_SIZE);
        final Map<String, Object> schemaClientProperties =
                getSchemaRegistryClientProperties(formatOptions);

        return new DefaultSchemaRegistryConfig(
                schemaRegistryURL, cacheSize, schemaId, schemaClientProperties);
    }

    /** Should be used in {@link FormatFactory#requiredOptions()}. */
    public static Set<ConfigOption<?>> getRequiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RegistryFormatOptions.URL);
        options.add(RegistryFormatOptions.SCHEMA_ID);
        options.add(RegistryFormatOptions.LOGICAL_CLUSTER_ID);
        options.add(RegistryFormatOptions.CREDENTIALS_SOURCE);
        return options;
    }

    /** Should be used in {@link FormatFactory#optionalOptions()}. */
    public static Set<ConfigOption<?>> getOptionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RegistryFormatOptions.SCHEMA_CACHE_SIZE);
        options.add(RegistryFormatOptions.BASIC_AUTH_USER_INFO);
        return options;
    }

    /** Should be used in {@link FormatFactory#forwardOptions()}. */
    public static Set<ConfigOption<?>> getForwardOptions() {
        return Stream.of(
                        RegistryFormatOptions.URL,
                        RegistryFormatOptions.SCHEMA_ID,
                        RegistryFormatOptions.SCHEMA_CACHE_SIZE,
                        RegistryFormatOptions.LOGICAL_CLUSTER_ID,
                        RegistryFormatOptions.BASIC_AUTH_USER_INFO,
                        RegistryFormatOptions.CREDENTIALS_SOURCE)
                .collect(Collectors.toSet());
    }

    private static Map<String, Object> getSchemaRegistryClientProperties(
            ReadableConfig formatOptions) {
        final Map<String, Object> properties = new HashMap<>();
        switch (formatOptions.get(RegistryFormatOptions.CREDENTIALS_SOURCE)) {
            case KEYS:
                properties.put("basic.auth.credentials.source", "USER_INFO");
                properties.put(
                        "basic.auth.user.info",
                        formatOptions.get(RegistryFormatOptions.BASIC_AUTH_USER_INFO));
                break;
            case DPAT:
                properties.put("bearer.auth.credentials.source", "OAUTHBEARER_DPAT");
                break;
        }
        properties.put(
                DPATCredentialProvider.LOGICAL_CLUSTER_PROPERTY,
                formatOptions.get(RegistryFormatOptions.LOGICAL_CLUSTER_ID));
        return properties;
    }

    /** Default implementation of {@link SchemaRegistryConfig}. */
    private static final class DefaultSchemaRegistryConfig implements SchemaRegistryConfig {

        private final String schemaRegistryUrl;
        private final int identityMapCapacity;
        private final int schemaId;
        private final Map<String, Object> properties;

        DefaultSchemaRegistryConfig(
                String schemaRegistryUrl,
                int identityMapCapacity,
                int schemaId,
                Map<String, Object> properties) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.identityMapCapacity = identityMapCapacity;
            this.schemaId = schemaId;
            this.properties = properties;
        }

        @Override
        public int getSchemaId() {
            return schemaId;
        }

        @Override
        public SchemaRegistryClient createClient(@Nullable JobID jobID) {
            if (jobID != null) {
                properties.put(DPATCredentialProvider.JOB_ID_PROPERTY, jobID.toHexString());
            }
            return new CachedSchemaRegistryClient(
                    schemaRegistryUrl,
                    identityMapCapacity,
                    Arrays.asList(
                            new AvroSchemaProvider(),
                            new JsonSchemaProvider(),
                            new ProtobufSchemaProvider()),
                    properties);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DefaultSchemaRegistryConfig that = (DefaultSchemaRegistryConfig) o;
            return identityMapCapacity == that.identityMapCapacity
                    && schemaId == that.schemaId
                    && Objects.equals(schemaRegistryUrl, that.schemaRegistryUrl);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaRegistryUrl, identityMapCapacity, schemaId);
        }
    }
}
