/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.annotation.Confluent;
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
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.flink.formats.registry.RegistryClientConfigFactory;
import io.confluent.flink.formats.registry.SchemaRegistryConfig;
import io.confluent.flink.formats.registry.json.JsonRegistrySerializationSchema.ValidateMode;

import java.util.HashSet;
import java.util.Set;

/**
 * Table format factory for providing configured instances of Schema Registry JSON to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
@Confluent
public class JsonRegistryFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "json-registry";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final SchemaRegistryConfig registryConfig = RegistryClientConfigFactory.get(formatOptions);
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                return new JsonRegistryDeserializationSchema(
                        registryConfig, rowType, context.createTypeInformation(physicalDataType));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            Context context, ReadableConfig formatOptions) {
        final SchemaRegistryConfig registryConfig = RegistryClientConfigFactory.get(formatOptions);
        final ValidateMode validateMode =
                formatOptions.get(JsonFormatOptions.VALIDATE_WRITES)
                        ? ValidateMode.VALIDATE_BEFORE_WRITE
                        : ValidateMode.NONE;
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                return new JsonRegistrySerializationSchema(registryConfig, rowType, validateMode);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return RegistryClientConfigFactory.getRequiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> set =
                new HashSet<>(RegistryClientConfigFactory.getOptionalOptions());
        set.add(JsonFormatOptions.VALIDATE_WRITES);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        final Set<ConfigOption<?>> set =
                new HashSet<>(RegistryClientConfigFactory.getForwardOptions());
        set.add(JsonFormatOptions.VALIDATE_WRITES);
        return set;
    }
}
