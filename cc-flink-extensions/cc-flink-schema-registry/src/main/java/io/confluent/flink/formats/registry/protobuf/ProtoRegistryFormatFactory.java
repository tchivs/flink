/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.protobuf;

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

import java.util.HashSet;
import java.util.Set;

/**
 * Table format factory for providing configured instances of Schema Registry Protobuf to RowData
 * {@link SerializationSchema} and {@link DeserializationSchema}.
 */
@Confluent
public class ProtoRegistryFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "proto-registry";

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
                return new ProtoRegistryDeserializationSchema(
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
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                return new ProtoRegistrySerializationSchema(registryConfig, rowType);
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
        return new HashSet<>(RegistryClientConfigFactory.getOptionalOptions());
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return new HashSet<>(RegistryClientConfigFactory.getForwardOptions());
    }
}
