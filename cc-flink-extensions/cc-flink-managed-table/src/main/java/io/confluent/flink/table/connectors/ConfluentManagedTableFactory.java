/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
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

import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CredentialsSource;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableUtils.BoundedOptions;
import io.confluent.flink.table.connectors.ConfluentManagedTableUtils.StartupOptions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CHANGELOG_MODE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_BOOTSTRAP_SERVERS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_CONSUMER_GROUP_ID;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_CREDENTIALS_SOURCE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_LOGICAL_CLUSTER_ID;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_PROPERTIES;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_TOPIC;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_TRANSACTIONAL_ID_PREFIX;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KEY_FIELDS_PREFIX;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KEY_FORMAT;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_BOUNDED_MODE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_BOUNDED_SPECIFIC_OFFSETS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_BOUNDED_TIMESTAMP_MILLIS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_STARTUP_MODE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.VALUE_FIELDS_INCLUDE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.VALUE_FORMAT;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.createKeyFields;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.createKeyFormatProjection;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.createValueFormatProjection;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.getBoundedOptions;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.getStartupOptions;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.validateKeyFormat;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.validatePrimaryKey;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.validateScanBoundedMode;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.validateScanStartupMode;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.validateValueFormat;

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
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CHANGELOG_MODE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_BOUNDED_MODE);
        options.add(VALUE_FORMAT);
        options.add(KAFKA_TOPIC);
        options.add(KAFKA_BOOTSTRAP_SERVERS);
        options.add(KAFKA_LOGICAL_CLUSTER_ID);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_BOUNDED_SPECIFIC_OFFSETS);
        options.add(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(KAFKA_CREDENTIALS_SOURCE);
        options.add(KAFKA_PROPERTIES);
        options.add(KAFKA_CONSUMER_GROUP_ID);
        options.add(KAFKA_TRANSACTIONAL_ID_PREFIX);
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

    // --------------------------------------------------------------------------------------------
    // Common parameters
    // --------------------------------------------------------------------------------------------

    private static Properties getProperties(ReadableConfig options) {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", options.get(KAFKA_BOOTSTRAP_SERVERS));
        properties.put("confluent.kafka.logical.cluster.id", options.get(KAFKA_LOGICAL_CLUSTER_ID));
        if (options.get(KAFKA_CREDENTIALS_SOURCE) == CredentialsSource.DPAT) {
            properties.put("confluent.kafka.dpat.enabled", true);
        }
        options.getOptional(KAFKA_CONSUMER_GROUP_ID)
                .ifPresent(id -> properties.put("group.id", id));
        options.getOptional(KAFKA_PROPERTIES).ifPresent(properties::putAll);
        return properties;
    }

    private static DynamicTableParameters createDynamicTableParameters(
            Context context,
            TableFactoryHelper helper,
            @Nullable Format keyFormat,
            Format valueFormat) {
        final ReadableConfig options = helper.getOptions();

        final List<String> keyFields = createKeyFields(context.getCatalogTable());

        // The table mode is the overall mode of the table including the format.
        // If the format is insert-only, it's the connector that does the heavy lifting.
        final ManagedChangelogMode tableMode = options.get(CHANGELOG_MODE);
        final ChangelogMode formatMode = valueFormat.getChangelogMode();
        // This ensures that the table mode is equal to or a superset of the format mode.
        validateValueFormat(options, tableMode, formatMode);
        validateScanStartupMode(options);
        validateScanBoundedMode(options);
        validatePrimaryKey(context.getPrimaryKeyIndexes(), tableMode);
        validateKeyFormat(
                options,
                tableMode,
                context.getCatalogTable().getResolvedSchema().getColumnNames(),
                context.getPrimaryKeyIndexes(),
                keyFormat,
                keyFields);

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(options, physicalDataType, keyFields);

        final int[] valueProjection =
                createValueFormatProjection(options, physicalDataType, keyFields);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        final Properties properties = getProperties(options);

        final StartupOptions startupOptions = getStartupOptions(options);

        final BoundedOptions boundedOptions = getBoundedOptions(options);

        return new DynamicTableParameters(
                physicalDataType,
                keyProjection,
                valueProjection,
                keyPrefix,
                options.get(KAFKA_TOPIC),
                properties,
                startupOptions,
                boundedOptions,
                options.getOptional(KAFKA_TRANSACTIONAL_ID_PREFIX).orElse(null),
                tableMode,
                context.getObjectIdentifier().asSummaryString());
    }

    /** Set of parameters for the dynamic table. */
    static class DynamicTableParameters {
        final DataType physicalDataType;
        final int[] keyProjection;
        final int[] valueProjection;
        final @Nullable String keyPrefix;
        final String topic;
        final Properties properties;
        final StartupOptions startupOptions;
        final BoundedOptions boundedOptions;
        final @Nullable String transactionalIdPrefix;
        final ManagedChangelogMode tableMode;
        final String tableIdentifier;

        DynamicTableParameters(
                DataType physicalDataType,
                int[] keyProjection,
                int[] valueProjection,
                @Nullable String keyPrefix,
                String topic,
                Properties properties,
                StartupOptions startupOptions,
                BoundedOptions boundedOptions,
                @Nullable String transactionalIdPrefix,
                ManagedChangelogMode tableMode,
                String tableIdentifier) {
            this.physicalDataType = physicalDataType;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.keyPrefix = keyPrefix;
            this.topic = topic;
            this.properties = properties;
            this.startupOptions = startupOptions;
            this.boundedOptions = boundedOptions;
            this.transactionalIdPrefix = transactionalIdPrefix;
            this.tableMode = tableMode;
            this.tableIdentifier = tableIdentifier;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DynamicTableParameters that = (DynamicTableParameters) o;
            return physicalDataType.equals(that.physicalDataType)
                    && Arrays.equals(keyProjection, that.keyProjection)
                    && Arrays.equals(valueProjection, that.valueProjection)
                    && Objects.equals(keyPrefix, that.keyPrefix)
                    && topic.equals(that.topic)
                    && properties.equals(that.properties)
                    && startupOptions.equals(that.startupOptions)
                    && boundedOptions.equals(that.boundedOptions)
                    && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                    && tableMode == that.tableMode
                    && tableIdentifier.equals(that.tableIdentifier);
        }

        @Override
        public int hashCode() {
            int result =
                    Objects.hash(
                            physicalDataType,
                            keyPrefix,
                            topic,
                            properties,
                            startupOptions,
                            boundedOptions,
                            transactionalIdPrefix,
                            tableMode,
                            tableIdentifier);
            result = 31 * result + Arrays.hashCode(keyProjection);
            result = 31 * result + Arrays.hashCode(valueProjection);
            return result;
        }
    }
}
