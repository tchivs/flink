/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.BoundedMode;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.FieldsInclude;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_TOPIC;
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

/** Utilities for {@link ConfluentManagedTableFactory}. */
@Confluent
public class ConfluentManagedTableUtils {

    /** Kafka startup options. */
    static class StartupOptions {
        public final StartupMode startupMode;
        public final Map<KafkaTopicPartition, Long> specificOffsets;
        public final long startupTimestampMillis;

        StartupOptions(
                StartupMode startupMode,
                Map<KafkaTopicPartition, Long> specificOffsets,
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
    static class BoundedOptions {
        public final BoundedMode boundedMode;
        public final Map<KafkaTopicPartition, Long> specificOffsets;
        public final long boundedTimestampMillis;

        BoundedOptions(
                BoundedMode boundedMode,
                Map<KafkaTopicPartition, Long> specificOffsets,
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

    static StartupOptions getStartupOptions(ReadableConfig options) {
        final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
        final StartupMode startupMode =
                options.getOptional(SCAN_STARTUP_MODE)
                        .map(ConfluentManagedTableUtils::fromOption)
                        .orElse(StartupMode.GROUP_OFFSETS);
        if (startupMode == StartupMode.SPECIFIC_OFFSETS) {
            buildSpecificOffsets(
                    options,
                    SCAN_STARTUP_SPECIFIC_OFFSETS,
                    options.get(KAFKA_TOPIC),
                    specificOffsets);
        }

        return new StartupOptions(
                startupMode,
                specificOffsets,
                options.getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS).orElse(0L));
    }

    static BoundedOptions getBoundedOptions(ReadableConfig options) {
        final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
        final BoundedMode boundedMode = fromOption(options.get(SCAN_BOUNDED_MODE));
        if (boundedMode == BoundedMode.SPECIFIC_OFFSETS) {
            buildSpecificOffsets(
                    options,
                    SCAN_BOUNDED_SPECIFIC_OFFSETS,
                    options.get(KAFKA_TOPIC),
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
            Map<KafkaTopicPartition, Long> specificOffsets) {
        final Map<Integer, Long> offsetMap = parseSpecificOffsets(options, option);
        offsetMap.forEach(
                (partition, offset) -> {
                    final KafkaTopicPartition topicPartition =
                            new KafkaTopicPartition(topic, partition);
                    specificOffsets.put(topicPartition, offset);
                });
    }

    private static StartupMode fromOption(
            ConfluentManagedTableOptions.ScanStartupMode startupMode) {
        switch (startupMode) {
            case EARLIEST_OFFSET:
                return StartupMode.EARLIEST;
            case LATEST_OFFSET:
                return StartupMode.LATEST;
            case GROUP_OFFSETS:
                return StartupMode.GROUP_OFFSETS;
            case SPECIFIC_OFFSETS:
                return StartupMode.SPECIFIC_OFFSETS;
            case TIMESTAMP:
                return StartupMode.TIMESTAMP;

            default:
                throw new TableException(
                        "Unsupported startup mode. Validator should have checked that.");
        }
    }

    private static BoundedMode fromOption(
            ConfluentManagedTableOptions.ScanBoundedMode boundedMode) {
        switch (boundedMode) {
            case UNBOUNDED:
                return BoundedMode.UNBOUNDED;
            case LATEST_OFFSET:
                return BoundedMode.LATEST;
            case GROUP_OFFSETS:
                return BoundedMode.GROUP_OFFSETS;
            case TIMESTAMP:
                return BoundedMode.TIMESTAMP;
            case SPECIFIC_OFFSETS:
                return BoundedMode.SPECIFIC_OFFSETS;

            default:
                throw new TableException(
                        "Unsupported bounded mode. Validator should have checked that.");
        }
    }

    static int[] createValueFormatProjection(
            ReadableConfig options, DataType physicalDataType, @Nullable List<String> keyFields) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final FieldsInclude strategy = options.get(VALUE_FIELDS_INCLUDE);
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
                    createKeyFormatProjection(options, physicalDataType, keyFields);
            return physicalFields
                    .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                    .toArray();
        }
        throw new TableException("Unknown value fields strategy:" + strategy);
    }

    static int[] createKeyFormatProjection(
            ReadableConfig options, DataType physicalDataType, @Nullable List<String> keyFields) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final Optional<String> optionalKeyFormat = options.getOptional(KEY_FORMAT);

        if (!optionalKeyFormat.isPresent() || keyFields == null) {
            return new int[0];
        }

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final List<String> physicalFields = LogicalTypeChecks.getFieldNames(physicalType);
        return keyFields.stream()
                .mapToInt(
                        keyField -> {
                            final int pos = physicalFields.indexOf(keyField);
                            // check that field name exists
                            if (pos < 0) {
                                throw new ValidationException(
                                        String.format(
                                                "Could not find the field '%s' in the table schema for usage in PARTITIONED BY clause. "
                                                        + "A key field must be a regular, physical column. "
                                                        + "The following columns can be selected:\n"
                                                        + "%s",
                                                keyField, physicalFields));
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

    static @Nullable List<String> createKeyFields(ResolvedCatalogTable table) {
        final List<String> partitionKeys = table.getPartitionKeys();
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
        if (table.getResolvedSchema().getPrimaryKey().isPresent()) {
            return table.getResolvedSchema().getPrimaryKey().get().getColumns();
        }

        return null;
    }

    static void validateKeyFormat(
            ReadableConfig options,
            ManagedChangelogMode tableMode,
            List<String> columns,
            int[] primaryKeyIndexes,
            @Nullable Format keyDecodingFormat,
            @Nullable List<String> keyFields) {
        final boolean hasKeys = keyFields != null && keyFields.size() > 0;
        if (keyDecodingFormat == null) {
            if (tableMode == ManagedChangelogMode.UPSERT) {
                throw new ValidationException(
                        String.format(
                                "A key format '%s' must be defined when performing upserts.",
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
            final Set<String> primaryKeyNames =
                    IntStream.of(primaryKeyIndexes)
                            .mapToObj(columns::get)
                            .collect(Collectors.toSet());
            if (!primaryKeyNames.isEmpty() && !primaryKeyNames.containsAll(keyFields)) {
                throw new ValidationException(
                        String.format(
                                "Key fields in PARTITIONED BY must fully contain primary key columns %s "
                                        + "if a primary key is defined.",
                                primaryKeyNames));
            }
        }
    }

    static void validatePrimaryKey(int[] keys, ManagedChangelogMode tableMode) {
        if (tableMode == ManagedChangelogMode.UPSERT && keys.length == 0) {
            throw new ValidationException("An upsert table requires a PRIMARY KEY constraint.");
        }
    }

    static void validateValueFormat(
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

    static void validateScanStartupMode(ReadableConfig options) {
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
                                                        ConfluentManagedTableOptions.ScanStartupMode
                                                                .TIMESTAMP));
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
                                                        ConfluentManagedTableOptions.ScanStartupMode
                                                                .SPECIFIC_OFFSETS));
                                    }
                                    parseSpecificOffsets(options, SCAN_STARTUP_SPECIFIC_OFFSETS);
                                    break;
                            }
                        });
    }

    static void validateScanBoundedMode(ReadableConfig options) {
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
                                                        ConfluentManagedTableOptions.ScanBoundedMode
                                                                .TIMESTAMP));
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
                                                        ConfluentManagedTableOptions.ScanBoundedMode
                                                                .SPECIFIC_OFFSETS));
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

    private ConfluentManagedTableUtils() {
        // no instantiation
    }
}
