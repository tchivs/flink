/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.flink.formats.avro.registry.credentials.DPATCredentialProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Table format factory for providing configured instances of Schema Registry Avro to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
@Confluent
public class AvroRegistryFormatFactory
        implements SerializationFormatFactory, DeserializationFormatFactory {

    public static final String IDENTIFIER = "avro-registry";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final String schemaRegistryURL = formatOptions.get(AvroRegistryFormatOptions.URL);
        final int schemaId = formatOptions.get(AvroRegistryFormatOptions.SCHEMA_ID);
        final int cacheSize = formatOptions.get(AvroRegistryFormatOptions.SCHEMA_CACHE_SIZE);
        final Map<String, Object> schemaClientProperties =
                getSchemaRegistryClientProperties(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                return new AvroRegistryDeserializationSchema(
                        new DefaultSchemaRegistryConfig(
                                schemaRegistryURL, cacheSize, schemaId, schemaClientProperties),
                        rowType,
                        context.createTypeInformation(physicalDataType));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final String schemaRegistryURL = formatOptions.get(AvroRegistryFormatOptions.URL);
        final int schemaId = formatOptions.get(AvroRegistryFormatOptions.SCHEMA_ID);
        final int cacheSize = formatOptions.get(AvroRegistryFormatOptions.SCHEMA_CACHE_SIZE);
        final Map<String, Object> schemaClientProperties =
                getSchemaRegistryClientProperties(formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new AvroRegistrySerializationSchema(
                        new DefaultSchemaRegistryConfig(
                                schemaRegistryURL, cacheSize, schemaId, schemaClientProperties),
                        rowType);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    private Map<String, Object> getSchemaRegistryClientProperties(ReadableConfig formatOptions) {
        final Map<String, Object> properties = new HashMap<>();
        switch (formatOptions.get(AvroRegistryFormatOptions.CREDENTIALS_SOURCE)) {
            case KEYS:
                properties.put("basic.auth.credentials.source", "USER_INFO");
                properties.put(
                        "basic.auth.user.info",
                        formatOptions.get(AvroRegistryFormatOptions.BASIC_AUTH_USER_INFO));
                break;
            case DPAT:
                properties.put("bearer.auth.credentials.source", "OAUTHBEARER_DPAT");
                break;
        }
        properties.put(
                DPATCredentialProvider.LOGICAL_CLUSTER_PROPERTY,
                formatOptions.get(AvroRegistryFormatOptions.LOGICAL_CLUSTER_ID));
        return properties;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AvroRegistryFormatOptions.URL);
        options.add(AvroRegistryFormatOptions.SCHEMA_ID);
        options.add(AvroRegistryFormatOptions.LOGICAL_CLUSTER_ID);
        options.add(AvroRegistryFormatOptions.CREDENTIALS_SOURCE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AvroRegistryFormatOptions.SCHEMA_CACHE_SIZE);
        options.add(AvroRegistryFormatOptions.BASIC_AUTH_USER_INFO);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        AvroRegistryFormatOptions.URL,
                        AvroRegistryFormatOptions.SCHEMA_ID,
                        AvroRegistryFormatOptions.SCHEMA_CACHE_SIZE,
                        AvroRegistryFormatOptions.LOGICAL_CLUSTER_ID,
                        AvroRegistryFormatOptions.BASIC_AUTH_USER_INFO,
                        AvroRegistryFormatOptions.CREDENTIALS_SOURCE)
                .collect(Collectors.toSet());
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
                    schemaRegistryUrl, identityMapCapacity, properties);
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
