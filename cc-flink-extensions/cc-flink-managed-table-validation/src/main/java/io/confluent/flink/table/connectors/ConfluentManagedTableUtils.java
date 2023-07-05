/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CleanupPolicy;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CredentialsSource;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.FieldsInclude;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ScanBoundedMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ScanStartupMode;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CHANGELOG_MODE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_BOOTSTRAP_SERVERS;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_CONSUMER_GROUP_ID;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_CREDENTIALS_SOURCE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_LOGICAL_CLUSTER_ID;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_PROPERTIES;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_TOPIC;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CONFLUENT_KAFKA_TRANSACTIONAL_ID_PREFIX;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_CLEANUP_POLICY;
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

/** Utilities for Confluent-native tables. */
@Confluent
public class ConfluentManagedTableUtils {

    public static void validateDynamicTableParameters(
            String tableIdentifier,
            List<String> primaryKeys,
            List<String> partitionKeys,
            List<String> physicalColumns,
            ReadableConfig options,
            @Nullable Format keyFormat,
            Format valueFormat) {
        // If this call succeeds all parameters should be correct.
        createDynamicTableParameters(
                tableIdentifier,
                primaryKeys,
                partitionKeys,
                physicalColumns,
                options,
                keyFormat,
                valueFormat,
                null);
    }

    public static DynamicTableParameters createDynamicTableParameters(
            String tableIdentifier,
            List<String> primaryKeys,
            List<String> partitionKeys,
            List<String> physicalColumns,
            ReadableConfig options,
            @Nullable Format keyFormat,
            Format valueFormat,
            @Nullable DataType physicalDataType) {
        final List<String> keyFields = createKeyFields(primaryKeys, partitionKeys);

        // The table mode is the overall mode of the table including the format.
        // If the format is insert-only, it's the connector that does the heavy lifting.
        final ManagedChangelogMode tableMode = options.get(CHANGELOG_MODE);
        final ChangelogMode formatMode = valueFormat.getChangelogMode();
        // This ensures that the table mode is equal to or a superset of the format mode.
        validateValueFormat(options, tableMode, formatMode);
        validateScanStartupMode(options);
        validateScanBoundedMode(options);
        validatePrimaryKey(primaryKeys, tableMode);
        validateKeyFormat(options, tableMode, primaryKeys, keyFormat, keyFields);

        final int[] keyProjection = createKeyFormatProjection(options, physicalColumns, keyFields);

        final int[] valueProjection =
                createValueFormatProjection(options, physicalColumns, keyFields);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        final Properties properties = getProperties(options);

        final StartupOptions startupOptions = getStartupOptions(options);

        final BoundedOptions boundedOptions = getBoundedOptions(options);

        return new DynamicTableParameters(
                physicalDataType,
                keyProjection,
                valueProjection,
                keyPrefix,
                options.get(CONFLUENT_KAFKA_TOPIC),
                properties,
                startupOptions,
                boundedOptions,
                options.getOptional(CONFLUENT_KAFKA_TRANSACTIONAL_ID_PREFIX).orElse(null),
                tableMode,
                tableIdentifier);
    }

    /** Set of parameters for the dynamic table. */
    public static class DynamicTableParameters {
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

    /** Kafka topic partition mapping. */
    public static final class ScanTopicPartition {
        public final String topic;
        public final int partition;

        ScanTopicPartition(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ScanTopicPartition that = (ScanTopicPartition) o;
            return partition == that.partition && topic.equals(that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition);
        }
    }

    /** Kafka startup options. */
    public static final class StartupOptions {
        public final ScanStartupMode startupMode;
        public final Map<ScanTopicPartition, Long> specificOffsets;
        public final long startupTimestampMillis;

        StartupOptions(
                ScanStartupMode startupMode,
                Map<ScanTopicPartition, Long> specificOffsets,
                long startupTimestampMillis) {
            this.startupMode = startupMode;
            this.specificOffsets = specificOffsets;
            this.startupTimestampMillis = startupTimestampMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StartupOptions that = (StartupOptions) o;
            return startupTimestampMillis == that.startupTimestampMillis
                    && startupMode == that.startupMode
                    && specificOffsets.equals(that.specificOffsets);
        }

        @Override
        public int hashCode() {
            return Objects.hash(startupMode, specificOffsets, startupTimestampMillis);
        }
    }

    /** Kafka bounded options. */
    public static final class BoundedOptions {
        public final ScanBoundedMode boundedMode;
        public final Map<ScanTopicPartition, Long> specificOffsets;
        public final long boundedTimestampMillis;

        BoundedOptions(
                ScanBoundedMode boundedMode,
                Map<ScanTopicPartition, Long> specificOffsets,
                long boundedTimestampMillis) {
            this.boundedMode = boundedMode;
            this.specificOffsets = specificOffsets;
            this.boundedTimestampMillis = boundedTimestampMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BoundedOptions that = (BoundedOptions) o;
            return boundedTimestampMillis == that.boundedTimestampMillis
                    && boundedMode == that.boundedMode
                    && specificOffsets.equals(that.specificOffsets);
        }

        @Override
        public int hashCode() {
            return Objects.hash(boundedMode, specificOffsets, boundedTimestampMillis);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Supporting methods
    // --------------------------------------------------------------------------------------------

    private static StartupOptions getStartupOptions(ReadableConfig options) {
        final Map<ScanTopicPartition, Long> specificOffsets = new HashMap<>();
        final ScanStartupMode startupMode = options.get(SCAN_STARTUP_MODE);
        if (startupMode == ScanStartupMode.SPECIFIC_OFFSETS) {
            buildSpecificOffsets(
                    options,
                    SCAN_STARTUP_SPECIFIC_OFFSETS,
                    options.get(CONFLUENT_KAFKA_TOPIC),
                    specificOffsets);
        }

        return new StartupOptions(
                startupMode,
                specificOffsets,
                options.getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS).orElse(0L));
    }

    private static BoundedOptions getBoundedOptions(ReadableConfig options) {
        final Map<ScanTopicPartition, Long> specificOffsets = new HashMap<>();
        final ScanBoundedMode boundedMode = options.get(SCAN_BOUNDED_MODE);
        if (boundedMode == ScanBoundedMode.SPECIFIC_OFFSETS) {
            buildSpecificOffsets(
                    options,
                    SCAN_BOUNDED_SPECIFIC_OFFSETS,
                    options.get(CONFLUENT_KAFKA_TOPIC),
                    specificOffsets);
        }

        return new BoundedOptions(
                boundedMode,
                specificOffsets,
                options.getOptional(SCAN_BOUNDED_TIMESTAMP_MILLIS).orElse(0L));
    }

    private static void buildSpecificOffsets(
            ReadableConfig options,
            ConfigOption<List<Map<String, String>>> option,
            String topic,
            Map<ScanTopicPartition, Long> specificOffsets) {
        final Map<Integer, Long> offsetMap = parseSpecificOffsets(options, option);
        offsetMap.forEach(
                (partition, offset) -> {
                    final ScanTopicPartition topicPartition =
                            new ScanTopicPartition(topic, partition);
                    specificOffsets.put(topicPartition, offset);
                });
    }

    private static int[] createValueFormatProjection(
            ReadableConfig options,
            List<String> physicalColumns,
            @Nullable List<String> keyFields) {
        final IntStream physicalFields = IntStream.range(0, physicalColumns.size());

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final FieldsInclude strategy =
                options.getOptional(VALUE_FIELDS_INCLUDE).orElse(FieldsInclude.EXCEPT_KEY);
        if (strategy == FieldsInclude.ALL) {
            if (keyPrefix.length() > 0) {
                throw new ValidationException(
                        String.format(
                                "A key prefix is not allowed when option '%s' is set to '%s'. "
                                        + "Set it to '%s' instead to avoid field overlaps.",
                                VALUE_FIELDS_INCLUDE.key(),
                                FieldsInclude.ALL,
                                FieldsInclude.EXCEPT_KEY));
            }
            return physicalFields.toArray();
        } else if (strategy == FieldsInclude.EXCEPT_KEY) {
            final int[] keyProjection =
                    createKeyFormatProjection(options, physicalColumns, keyFields);
            return physicalFields
                    .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                    .toArray();
        }
        throw new TableException("Unknown value fields strategy:" + strategy);
    }

    private static int[] createKeyFormatProjection(
            ReadableConfig options,
            List<String> physicalColumns,
            @Nullable List<String> keyFields) {
        final Optional<String> optionalKeyFormat = options.getOptional(KEY_FORMAT);

        if (!optionalKeyFormat.isPresent() || keyFields == null) {
            return new int[0];
        }

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        return keyFields.stream()
                .mapToInt(
                        keyField -> {
                            final int pos = physicalColumns.indexOf(keyField);
                            // check that field name exists
                            if (pos < 0) {
                                throw new ValidationException(
                                        String.format(
                                                "Could not find the field '%s' in the table schema for usage in PARTITIONED BY clause. "
                                                        + "A key field must be a regular, physical column. "
                                                        + "The following columns can be selected:\n"
                                                        + "%s",
                                                keyField, physicalColumns));
                            }
                            // check that field name is prefixed correctly
                            if (!keyField.startsWith(keyPrefix)) {
                                throw new ValidationException(
                                        String.format(
                                                "All fields in PARTITIONED BY must be prefixed with '%s' when option '%s' "
                                                        + "is set but field '%s' is not prefixed.",
                                                keyPrefix, KEY_FIELDS_PREFIX.key(), keyField));
                            }
                            return pos;
                        })
                .toArray();
    }

    private static @Nullable List<String> createKeyFields(
            List<String> primaryKeys, List<String> partitionKeys) {
        if (!partitionKeys.isEmpty()) {
            final Set<String> duplicateColumns =
                    partitionKeys.stream()
                            .filter(name -> Collections.frequency(partitionKeys, name) > 1)
                            .collect(Collectors.toSet());
            if (!duplicateColumns.isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "PARTITIONED BY clause must not contain duplicate columns. Found: %s",
                                duplicateColumns));
            }

            return partitionKeys;
        }

        // regardless of the mode, it makes sense to partition by primary key such that efficient
        // point lookups can be implemented
        if (!primaryKeys.isEmpty()) {
            return primaryKeys;
        }

        return null;
    }

    private static void validateKeyFormat(
            ReadableConfig options,
            ManagedChangelogMode tableMode,
            List<String> primaryKeys,
            @Nullable Format keyDecodingFormat,
            @Nullable List<String> partitionKeys) {
        final CleanupPolicy cleanupPolicy = options.get(KAFKA_CLEANUP_POLICY);
        final boolean isCompacted =
                cleanupPolicy == CleanupPolicy.DELETE_COMPACT
                        || cleanupPolicy == CleanupPolicy.COMPACT;
        final boolean isUpsertLike = tableMode == ManagedChangelogMode.UPSERT || isCompacted;
        final boolean hasKeys = partitionKeys != null && partitionKeys.size() > 0;
        if (keyDecodingFormat == null) {
            if (isUpsertLike) {
                throw new ValidationException(
                        String.format(
                                "A key format '%s' must be defined when performing upserts or compaction.",
                                KEY_FORMAT.key()));
            }
            if (hasKeys) {
                throw new ValidationException(
                        String.format(
                                "PARTITIONED BY and PRIMARY KEY clauses require a key format '%s'.",
                                KEY_FORMAT.key()));
            }
        } else {
            if (!hasKeys) {
                throw new ValidationException(
                        String.format(
                                "A key format '%s' requires the declaration of one or more of key fields "
                                        + "using PARTITIONED BY (or PRIMARY KEY if applicable).",
                                KEY_FORMAT.key()));
            }
            final ChangelogMode keyFormatMode = keyDecodingFormat.getChangelogMode();
            if (!keyFormatMode.containsOnly(RowKind.INSERT)) {
                throw new ValidationException(
                        String.format(
                                "A key format should only deal with INSERT-only records. "
                                        + "But %s has a changelog mode of %s.",
                                options.get(KEY_FORMAT), keyFormatMode));
            }
            final Set<String> primaryKeySet = new HashSet<>(primaryKeys);
            if (!primaryKeySet.isEmpty()) {
                if (!primaryKeySet.containsAll(partitionKeys)) {
                    throw new ValidationException(
                            String.format(
                                    "Key fields in PARTITIONED BY must fully contain primary key columns %s "
                                            + "if a primary key is defined.",
                                    primaryKeySet));
                }
                final Set<String> partitionKeySet = new HashSet<>(partitionKeys);
                // Even if the table mode is not upsert, if compaction is enabled it behaves similar
                // to upsert. Therefore, we must guard the changelog by not allowing custom
                // partitioning in case upsert is enabled later.
                if (isUpsertLike && !primaryKeySet.equals(partitionKeySet)) {
                    throw new ValidationException(
                            String.format(
                                    "A custom PARTITIONED BY clause is not allowed if upserts or "
                                            + "compaction are enabled. The partitioning key must "
                                            + "be equal to the primary key %s.",
                                    primaryKeySet));
                }
            }
        }
    }

    private static void validatePrimaryKey(
            List<String> primaryKeys, ManagedChangelogMode tableMode) {
        if (tableMode == ManagedChangelogMode.UPSERT && primaryKeys.isEmpty()) {
            throw new ValidationException("An upsert table requires a PRIMARY KEY constraint.");
        }
    }

    private static void validateValueFormat(
            ReadableConfig options, ManagedChangelogMode tableMode, ChangelogMode formatMode) {
        if (!formatMode.containsOnly(RowKind.INSERT)
                && !formatMode.equals(tableMode.toChangelogMode())) {
            throw new ValidationException(
                    String.format(
                            "If the value format produces updates, the table must have a matching changelog mode. "
                                    + "The value format must be INSERT-only otherwise. "
                                    + "But '%s' has a changelog mode of %s, whereas the table has a changelog mode of %s.",
                            options.get(VALUE_FORMAT), formatMode, tableMode));
        }
    }

    private static void validateScanStartupMode(ReadableConfig options) {
        options.getOptional(SCAN_STARTUP_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!options.getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                                        ScanStartupMode.TIMESTAMP));
                                    }
                                    break;
                                case SPECIFIC_OFFSETS:
                                    if (!options.getOptional(SCAN_STARTUP_SPECIFIC_OFFSETS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
                                                        ScanStartupMode.SPECIFIC_OFFSETS));
                                    }
                                    parseSpecificOffsets(options, SCAN_STARTUP_SPECIFIC_OFFSETS);
                                    break;
                            }
                        });
    }

    private static void validateScanBoundedMode(ReadableConfig options) {
        options.getOptional(SCAN_BOUNDED_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!options.getOptional(SCAN_BOUNDED_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' bounded mode"
                                                                + " but missing.",
                                                        SCAN_BOUNDED_TIMESTAMP_MILLIS.key(),
                                                        ScanBoundedMode.TIMESTAMP));
                                    }

                                    break;
                                case SPECIFIC_OFFSETS:
                                    if (!options.getOptional(SCAN_BOUNDED_SPECIFIC_OFFSETS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' bounded mode"
                                                                + " but missing.",
                                                        SCAN_BOUNDED_SPECIFIC_OFFSETS.key(),
                                                        ScanBoundedMode.SPECIFIC_OFFSETS));
                                    }
                                    parseSpecificOffsets(options, SCAN_BOUNDED_SPECIFIC_OFFSETS);
                                    break;
                            }
                        });
    }

    private static Map<Integer, Long> parseSpecificOffsets(
            ReadableConfig options, ConfigOption<List<Map<String, String>>> option) {
        final String errorMessage =
                String.format(
                        "Invalid value for key '%s'. "
                                + "Specific offsets should follow the format 'partition:0,offset:42;partition:1,offset:300'.",
                        option.key());

        final List<Map<String, String>> pairs = options.get(option);

        final Map<Integer, Long> offsetMap = new HashMap<>();
        for (Map<String, String> pair : pairs) {
            final String unparsedPartition = pair.get("partition");
            final String unparsedOffset = pair.get("offset");
            if (unparsedPartition == null || unparsedOffset == null) {
                throw new ValidationException(errorMessage);
            }
            try {
                final Integer partition = Integer.valueOf(unparsedPartition);
                final Long offset = Long.valueOf(unparsedOffset);
                offsetMap.put(partition, offset);
            } catch (NumberFormatException e) {
                throw new ValidationException(errorMessage, e);
            }
        }
        return offsetMap;
    }

    /** This sets all default properties for Kafka. */
    private static Properties getProperties(ReadableConfig options) {
        final Properties properties = new Properties();
        options.getOptional(CONFLUENT_KAFKA_BOOTSTRAP_SERVERS)
                .ifPresent(servers -> properties.put("bootstrap.servers", servers));
        options.getOptional(CONFLUENT_KAFKA_LOGICAL_CLUSTER_ID)
                .ifPresent(lkc -> properties.put("confluent.kafka.logical.cluster.id", lkc));
        if (options.get(CONFLUENT_KAFKA_CREDENTIALS_SOURCE) == CredentialsSource.DPAT) {
            properties.put("confluent.kafka.dpat.enabled", "true");
        }
        options.getOptional(CONFLUENT_KAFKA_CONSUMER_GROUP_ID)
                .ifPresent(id -> properties.put("group.id", id));

        // Maximum transaction timeout (15 min) as allowed by CCloud
        properties.setProperty("transaction.timeout.ms", "900000");

        // Note: Make sure to set default properties before this line is applied in order to
        // allow DevOps overwriting defaults via the CompiledPlan if necessary.
        options.getOptional(CONFLUENT_KAFKA_PROPERTIES).ifPresent(properties::putAll);
        return properties;
    }

    private ConfluentManagedTableUtils() {
        // no instantiation
    }
}
