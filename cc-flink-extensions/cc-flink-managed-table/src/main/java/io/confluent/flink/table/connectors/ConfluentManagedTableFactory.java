/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.ManagedTableFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableUtils.DynamicTableParameters;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CHANGELOG_MODE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_API_CLIENT_BASE_PATH;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_CLOUD_ENV;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_CLOUD_ORG;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_CTS_ENABLED;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_BOOTSTRAP_SERVERS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_CLIENT_ID_PREFIX;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_CONSUMER_GROUP_ID;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_CREDENTIALS_SOURCE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_LOGICAL_CLUSTER_ID;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_PROPERTIES;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_REPLICATION_FACTOR;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_TOPIC;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_TRANSACTIONAL_ID_PREFIX;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_SOURCE_WATERMARK_EMIT_PER_ROW;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_SOURCE_WATERMARK_VERSION;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KEY_FIELDS_PREFIX;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KEY_FORMAT;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.PUBLIC_CREATION_OPTIONS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_BOUNDED_MODE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_BOUNDED_SPECIFIC_OFFSETS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_BOUNDED_TIMESTAMP_MILLIS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_STARTUP_MODE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.VALUE_FIELDS_INCLUDE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.VALUE_FORMAT;

/**
 * {@link ManagedTableFactory} for a Confluent-native table.
 *
 * <p>The table aims to abstract the storage layer and focuses on the SQL semantics of the table
 * rather than connector specifics. Thus, it unifies the open source Kafka connectors which only
 * support append and upsert mode, and uses the {@code PARTITIONED BY} clause for defining Kafka
 * message keys. It can produce and consume all kinds of changes (see also {@link
 * ManagedChangelogMode}) in a unified connector.
 *
 * <p>This factory does not contain a full validation layer for options. It only contains some basic
 * assertion checks. It assumes that the SQL metastore has performed this task. Thus, options are in
 * a holistic format.
 *
 * <p>For optional options, this factory will provide default values. For required options, the
 * metastore should have added defaults.
 */
@Confluent
public class ConfluentManagedTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "confluent";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // Must contain all *required runtime specific* options (public and private).
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CHANGELOG_MODE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_BOUNDED_MODE);
        options.add(VALUE_FORMAT);
        options.add(CONFLUENT_KAFKA_TOPIC);
        options.add(CONFLUENT_KAFKA_BOOTSTRAP_SERVERS);
        options.add(CONFLUENT_KAFKA_LOGICAL_CLUSTER_ID);
        // Accepts *creation specific options* (public)
        options.addAll(PUBLIC_CREATION_OPTIONS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // Must contain all *optional runtime specific* options (public and private).
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_BOUNDED_SPECIFIC_OFFSETS);
        options.add(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(CONFLUENT_KAFKA_CREDENTIALS_SOURCE);
        options.add(CONFLUENT_KAFKA_REPLICATION_FACTOR);
        options.add(CONFLUENT_KAFKA_PROPERTIES);
        options.add(CONFLUENT_KAFKA_CONSUMER_GROUP_ID);
        options.add(CONFLUENT_KAFKA_CLIENT_ID_PREFIX);
        options.add(CONFLUENT_KAFKA_TRANSACTIONAL_ID_PREFIX);
        options.add(CONFLUENT_SOURCE_WATERMARK_VERSION);
        options.add(CONFLUENT_SOURCE_WATERMARK_EMIT_PER_ROW);
        // Confluent Table Store specific options
        options.add(CONFLUENT_CTS_ENABLED);
        options.add(CONFLUENT_CLOUD_ORG);
        options.add(CONFLUENT_CLOUD_ENV);
        options.add(CONFLUENT_API_CLIENT_BASE_PATH);
        return options;
    }

    // --------------------------------------------------------------------------------------------
    // Factory methods
    // --------------------------------------------------------------------------------------------

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
                helper.discoverOptionalDecodingFormat(
                                DeserializationFormatFactory.class, KEY_FORMAT)
                        .orElse(null);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, VALUE_FORMAT);

        helper.validate();

        return new ConfluentManagedTableSource(
                createDynamicTableParameters(
                        context, helper, keyDecodingFormat, valueDecodingFormat),
                keyDecodingFormat,
                valueDecodingFormat);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
                helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT)
                        .orElse(null);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, VALUE_FORMAT);

        helper.validate();

        return new ConfluentManagedTableSink(
                createDynamicTableParameters(
                        context, helper, keyEncodingFormat, valueEncodingFormat),
                keyEncodingFormat,
                valueEncodingFormat);
    }

    private static DynamicTableParameters createDynamicTableParameters(
            Context context,
            TableFactoryHelper helper,
            @Nullable Format keyFormat,
            Format valueFormat) {
        return ConfluentManagedTableUtils.createDynamicTableParameters(
                context.getConfiguration(),
                context.getObjectIdentifier().asSummaryString(),
                context.getCatalogTable()
                        .getResolvedSchema()
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElse(Collections.emptyList()),
                context.getCatalogTable().getPartitionKeys(),
                DataType.getFieldNames(context.getPhysicalRowDataType()),
                helper.getOptions(),
                keyFormat,
                valueFormat,
                context.getPhysicalRowDataType());
    }
}
