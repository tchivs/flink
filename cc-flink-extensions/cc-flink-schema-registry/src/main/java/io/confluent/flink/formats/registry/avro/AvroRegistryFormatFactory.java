/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.avro;

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
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.flink.formats.registry.RegistryClientConfigFactory;
import io.confluent.flink.formats.registry.SchemaRegistryConfig;

import java.util.Set;

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

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType physicalDataType) {
                final SchemaRegistryConfig registryConfig =
                        RegistryClientConfigFactory.get(formatOptions);

                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                return new AvroRegistryDeserializationSchema(
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
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final SchemaRegistryConfig registryConfig =
                        RegistryClientConfigFactory.get(formatOptions);

                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new AvroRegistrySerializationSchema(registryConfig, rowType);
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
        return RegistryClientConfigFactory.getOptionalOptions();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return RegistryClientConfigFactory.getForwardOptions();
    }
}
