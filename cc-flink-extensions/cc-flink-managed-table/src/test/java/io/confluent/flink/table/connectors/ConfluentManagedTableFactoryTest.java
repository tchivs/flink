/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ScanBoundedMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ScanStartupMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableUtils.DynamicTableParameters;
import io.confluent.flink.table.connectors.ConfluentManagedTableUtils.ScanTopicPartition;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConfluentManagedTableFactory}. */
@Confluent
public class ConfluentManagedTableFactoryTest {

    private static final String KEY_K1 = "key_k1";
    private static final String KEY_K2 = "key_k2";
    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);

    private static final ResolvedSchema SCHEMA_WITHOUT_PK =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(NAME, DataTypes.STRING().notNull()),
                            Column.computed(
                                    COMPUTED_COLUMN_NAME,
                                    ResolvedExpressionMock.of(
                                            COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION)),
                            Column.physical(KEY_K1, DataTypes.STRING().notNull()),
                            Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
                            Column.physical(TIME, DataTypes.TIMESTAMP(3)),
                            Column.physical(KEY_K2, DataTypes.STRING().notNull())),
                    Collections.singletonList(
                            WatermarkSpec.of(
                                    TIME,
                                    ResolvedExpressionMock.of(
                                            WATERMARK_DATATYPE, WATERMARK_EXPRESSION))),
                    null);

    private static final ResolvedSchema SCHEMA_WITH_PK =
            new ResolvedSchema(
                    SCHEMA_WITHOUT_PK.getColumns(),
                    SCHEMA_WITHOUT_PK.getWatermarkSpecs(),
                    UniqueConstraint.primaryKey("pk", Arrays.asList(KEY_K1, KEY_K2)));

    @Nested
    class RetractTests {

        @Test
        void testComplexOptions() {
            final Map<String, String> options = getComplexOptions();

            final Consumer<DynamicTableParameters> testParameters =
                    (parameters) -> {
                        assertThat(parameters.tableMode).isEqualTo(ManagedChangelogMode.RETRACT);
                        assertThat(parameters.startupOptions.startupMode)
                                .isEqualTo(ScanStartupMode.SPECIFIC_OFFSETS);
                        assertThat(parameters.startupOptions.specificOffsets)
                                .containsEntry(new ScanTopicPartition("MyTopic", 0), 1L)
                                .containsEntry(new ScanTopicPartition("MyTopic", 1), 0L)
                                .containsEntry(new ScanTopicPartition("MyTopic", 2), 42L);
                        assertThat(parameters.boundedOptions.boundedMode)
                                .isEqualTo(ScanBoundedMode.TIMESTAMP);
                        assertThat(parameters.boundedOptions.boundedTimestampMillis)
                                .isEqualTo(100001L);
                        assertThat(parameters.keyProjection).containsExactly(1, 4);
                        assertThat(parameters.keyPrefix).isEqualTo("key_");
                        assertThat(parameters.valueProjection).containsExactly(0, 2, 3);
                        assertThat(parameters.topic).isEqualTo("MyTopic");
                        assertThat(parameters.properties)
                                .containsEntry("bootstrap.servers", "localhost:8080")
                                .containsEntry("confluent.kafka.logical.cluster.id", "lkc-4242")
                                .containsEntry("group.id", "generated_consumer_id_4242");
                        assertThat(parameters.transactionalIdPrefix)
                                .isEqualTo("generated_transact_id_4242");
                    };

            final ConfluentManagedTableSource source =
                    createTableSource(SCHEMA_WITHOUT_PK, options, Arrays.asList(KEY_K1, KEY_K2));
            testParameters.accept(source.getParameters());

            final ConfluentManagedTableSink sink =
                    createTableSink(SCHEMA_WITHOUT_PK, options, Arrays.asList(KEY_K1, KEY_K2));
            testParameters.accept(sink.getParameters());
        }

        @Test
        void testComplexOptionsWithoutUniqueIdentifiers() {
            // The factory should also produce a table without these properties.
            // Unique IDs can be created in a later stage.
            final Map<String, String> options = getComplexOptions();
            options.remove("confluent.kafka.consumer-group-id");
            options.remove("confluent.kafka.transactional-id-prefix");

            final Consumer<DynamicTableParameters> testParameters =
                    (parameters) -> {
                        assertThat(parameters.properties)
                                .doesNotContainEntry("group.id", "generated_consumer_id_4242");
                        assertThat(parameters.transactionalIdPrefix).isNull();
                    };

            final ConfluentManagedTableSource source =
                    createTableSource(SCHEMA_WITH_PK, options, Arrays.asList(KEY_K1, KEY_K2));
            testParameters.accept(source.getParameters());
            assertThat(source.getChangelogMode()).isEqualTo(ChangelogMode.all());

            final ConfluentManagedTableSink sink =
                    createTableSink(SCHEMA_WITH_PK, options, Arrays.asList(KEY_K1, KEY_K2));
            testParameters.accept(sink.getParameters());
            assertThat(sink.getChangelogMode(ChangelogMode.insertOnly()))
                    .isEqualTo(ChangelogMode.all());
        }

        @Test
        void testWithoutPartitionKeys() {
            final Map<String, String> options = getComplexOptions();
            options.remove("key.format");

            final ConfluentManagedTableSource source =
                    createTableSource(SCHEMA_WITHOUT_PK, options, Collections.emptyList());
            final DynamicTableParameters parameters = source.getParameters();
            assertThat(parameters.tableMode).isEqualTo(ManagedChangelogMode.RETRACT);
            // no partition keys set
            assertThat(parameters.keyProjection).isEmpty();
        }

        @Test
        void testWithImplicitPartitionKeys() {
            final Map<String, String> options = getComplexOptions();

            final ConfluentManagedTableSource source =
                    createTableSource(SCHEMA_WITH_PK, options, Collections.emptyList());
            final DynamicTableParameters parameters = source.getParameters();
            assertThat(parameters.tableMode).isEqualTo(ManagedChangelogMode.RETRACT);
            // partition keys are derived from primary key
            assertThat(parameters.keyProjection).containsExactly(1, 4);
        }

        @Test
        void testInvalidKeyFormatWithPartitionKey() {
            testError(
                    SCHEMA_WITHOUT_PK,
                    (options, keys) -> {
                        options.remove("key.format");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                    },
                    "PARTITIONED BY and PRIMARY KEY clauses require a key format 'key.format'.");
        }

        @Test
        void testInvalidUpdatingKeyFormat() {
            testError(
                    SCHEMA_WITHOUT_PK,
                    (options, keys) -> {
                        options.put("key.format", TestFormatFactory.IDENTIFIER);
                        options.put("key.test-format.delimiter", ",");
                        options.put("key.test-format.changelog-mode", "I;UA;UB;D");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                    },
                    "A key format should only deal with INSERT-only records.");
        }

        @Test
        void testMissingPartitionKeys() {
            testError(
                    SCHEMA_WITHOUT_PK,
                    (options, keys) -> {
                        // no partition keys but a key format
                    },
                    "A key format 'key.format' requires the declaration of one or more of key "
                            + "fields using PARTITIONED BY (or PRIMARY KEY if applicable).");
        }

        @Test
        void testInvalidPartitionKeyPrefix() {
            testError(
                    SCHEMA_WITHOUT_PK,
                    (options, keys) -> {
                        options.put("key.fields-prefix", "_");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                    },
                    "All fields in PARTITIONED BY must be prefixed with '_' when "
                            + "option 'key.fields-prefix' is set but field 'key_k1' is not prefixed.");
        }

        @Test
        void testInvalidPartitionKey() {
            testError(
                    SCHEMA_WITH_PK,
                    (options, keys) -> keys.add("invalid"),
                    "Key fields in PARTITIONED BY must fully contain primary key columns [key_k1, key_k2] "
                            + "if a primary key is defined.");
        }

        @Test
        void testInvalidFieldsIncludeCombination() {
            testError(
                    SCHEMA_WITHOUT_PK,
                    (options, keys) -> {
                        options.put("value.fields-include", "all");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                    },
                    "A key prefix is not allowed when option 'value.fields-include' is set to 'all'.");
        }

        @Test
        void testDuplicatePartitionKey() {
            testError(
                    SCHEMA_WITHOUT_PK,
                    (options, keys) -> {
                        options.put("value.format", TestFormatFactory.IDENTIFIER);
                        options.put("value.test-format.delimiter", ",");
                        options.put("value.test-format.changelog-mode", "I;UA;UB;D");
                        keys.add(KEY_K1);
                        keys.add(KEY_K1);
                    },
                    "PARTITIONED BY clause must not contain duplicate columns. Found: [key_k1]");
        }
    }

    @Nested
    class UpsertTests {

        @Test
        void testWithImplicitPartitionKeys() {
            final Map<String, String> options = getComplexOptions();
            options.put("changelog.mode", "upsert");

            final Consumer<DynamicTableParameters> testParameters =
                    (parameters) -> {
                        assertThat(parameters.tableMode).isEqualTo(ManagedChangelogMode.UPSERT);
                        // key projection is inserted implicitly although no partition keys are set
                        assertThat(parameters.keyProjection).containsExactly(1, 4);
                    };

            final ConfluentManagedTableSource source =
                    createTableSource(SCHEMA_WITH_PK, options, Collections.emptyList());
            testParameters.accept(source.getParameters());
            // changelog mode considers Kafka specifics in source
            assertThat(source.getChangelogMode())
                    .isEqualTo(
                            ChangelogMode.newBuilder()
                                    .addContainedKind(RowKind.UPDATE_AFTER)
                                    .addContainedKind(RowKind.DELETE)
                                    .build());

            final ConfluentManagedTableSink sink =
                    createTableSink(SCHEMA_WITH_PK, options, Collections.emptyList());
            testParameters.accept(source.getParameters());
            // changelog mode considers Kafka specifics in sink
            assertThat(sink.getChangelogMode(ChangelogMode.insertOnly()))
                    .isEqualTo(
                            ChangelogMode.newBuilder()
                                    .addContainedKind(RowKind.INSERT)
                                    .addContainedKind(RowKind.UPDATE_AFTER)
                                    .addContainedKind(RowKind.DELETE)
                                    .build());
        }

        @Test
        void testRetractValueFormat() {
            testError(
                    SCHEMA_WITH_PK,
                    (options, keys) -> {
                        options.put("changelog.mode", "upsert");
                        options.remove("key.format");
                        options.put("value.format", TestFormatFactory.IDENTIFIER);
                        options.put("value.test-format.delimiter", ",");
                        options.put("value.test-format.changelog-mode", "I;UA;UB;D");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                    },
                    "If the value format produces updates, the table must have a matching changelog mode.");
        }

        @Test
        void testMissingKeyFormat() {
            testError(
                    SCHEMA_WITH_PK,
                    (options, keys) -> {
                        options.put("changelog.mode", "upsert");
                        options.remove("key.format");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                    },
                    "A key format 'key.format' must be defined when performing upserts.");
        }

        @Test
        void testInvalidAdditionalPartitionKey() {
            testError(
                    SCHEMA_WITH_PK,
                    (options, keys) -> {
                        options.put("changelog.mode", "upsert");
                        options.remove("key.fields-prefix");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                        keys.add(NAME);
                    },
                    "Key fields in PARTITIONED BY must fully contain primary key columns [key_k1, key_k2]"
                            + " if a primary key is defined.");
        }

        @Test
        void testMissingPrimaryKey() {
            testError(
                    SCHEMA_WITHOUT_PK,
                    (options, keys) -> {
                        options.put("changelog.mode", "upsert");
                        options.remove("key.fields-prefix");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                        keys.add(NAME);
                    },
                    "An upsert table requires a PRIMARY KEY constraint.");
        }

        @Test
        void testInvalidCustomPartitioningWithCompaction() {
            testError(
                    SCHEMA_WITH_PK,
                    (options, keys) -> {
                        options.put("changelog.mode", "upsert");
                        options.put("kafka.cleanup-policy", "compact");
                        keys.add(KEY_K1);
                    },
                    "A custom PARTITIONED BY clause is not allowed if compaction is enabled in "
                            + "upsert mode. The compaction key must be equal to the primary key "
                            + "[key_k1, key_k2] which is used for upserts.");
        }
    }

    @Nested
    class AppendTests {

        @Test
        void testAppend() {
            final Map<String, String> options = getComplexOptions();
            options.put("changelog.mode", "append");

            final Consumer<DynamicTableParameters> testParameters =
                    (parameters) -> {
                        assertThat(parameters.tableMode).isEqualTo(ManagedChangelogMode.APPEND);
                        // according to partition keys
                        assertThat(parameters.keyProjection).containsExactly(1, 4);
                    };

            final ConfluentManagedTableSource source =
                    createTableSource(SCHEMA_WITHOUT_PK, options, Arrays.asList(KEY_K1, KEY_K2));
            testParameters.accept(source.getParameters());
            assertThat(source.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());

            final ConfluentManagedTableSink sink =
                    createTableSink(SCHEMA_WITHOUT_PK, options, Arrays.asList(KEY_K1, KEY_K2));
            testParameters.accept(source.getParameters());
            assertThat(sink.getChangelogMode(ChangelogMode.insertOnly()))
                    .isEqualTo(ChangelogMode.insertOnly());
        }

        @Test
        void testWithoutPartitionKeys() {
            final Map<String, String> options = getComplexOptions();
            options.put("changelog.mode", "append");
            options.remove("key.format");

            final ConfluentManagedTableSource source =
                    createTableSource(SCHEMA_WITHOUT_PK, options, Collections.emptyList());
            final DynamicTableParameters parameters = source.getParameters();
            assertThat(parameters.tableMode).isEqualTo(ManagedChangelogMode.APPEND);
            // no partition keys set
            assertThat(parameters.keyProjection).isEmpty();
        }

        @Test
        void testWithImplicitPartitionKeys() {
            final Map<String, String> options = getComplexOptions();
            options.put("changelog.mode", "append");

            final ConfluentManagedTableSource source =
                    createTableSource(SCHEMA_WITH_PK, options, Collections.emptyList());
            final DynamicTableParameters parameters = source.getParameters();
            assertThat(parameters.tableMode).isEqualTo(ManagedChangelogMode.APPEND);
            // partition keys are derived from primary key
            assertThat(parameters.keyProjection).containsExactly(1, 4);
        }

        @Test
        void testInvalidKeyFormatWithPartitionKey() {
            testError(
                    SCHEMA_WITHOUT_PK,
                    (options, keys) -> {
                        options.put("changelog.mode", "append");
                        options.remove("key.format");
                        keys.add(KEY_K1);
                        keys.add(KEY_K2);
                    },
                    "PARTITIONED BY and PRIMARY KEY clauses require a key format 'key.format'.");
        }
    }

    private static Map<String, String> getComplexOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "confluent");
        options.put("changelog.mode", "retract");
        options.put("scan.startup.mode", "specific-offsets");
        options.put(
                "scan.startup.specific-offsets",
                "partition: 0, offset: 1; partition: 2, offset: 42; partition: 1, offset: 0;");
        options.put("scan.bounded.mode", "timestamp");
        options.put("scan.bounded.timestamp-millis", "100001");
        options.put("key.format", "raw");
        options.put("key.fields-prefix", "key_");
        options.put("value.format", "raw");
        options.put("value.fields-include", "except-key");
        options.put("kafka.cleanup-policy", "delete");
        options.put("kafka.partitions", "6");
        options.put("kafka.retention.time", "7 d");
        options.put("kafka.retention.size", "0");
        options.put("kafka.max-message-size", "2097164 bytes");
        options.put("confluent.kafka.topic", "MyTopic");
        options.put("confluent.kafka.bootstrap-servers", "localhost:8080");
        options.put("confluent.kafka.logical-cluster-id", "lkc-4242");
        options.put("confluent.kafka.consumer-group-id", "generated_consumer_id_4242");
        options.put("confluent.kafka.transactional-id-prefix", "generated_transact_id_4242");
        return options;
    }

    private static ConfluentManagedTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options, List<String> partitionKeys) {
        final ConfluentManagedTableFactory factory = new ConfluentManagedTableFactory();
        final DynamicTableFactory.Context context =
                createFactoryContext(schema, partitionKeys, options);
        return (ConfluentManagedTableSource) factory.createDynamicTableSource(context);
    }

    private static ConfluentManagedTableSink createTableSink(
            ResolvedSchema schema, Map<String, String> options, List<String> partitionKeys) {
        final ConfluentManagedTableFactory factory = new ConfluentManagedTableFactory();
        final DynamicTableFactory.Context context =
                createFactoryContext(schema, partitionKeys, options);
        return (ConfluentManagedTableSink) factory.createDynamicTableSink(context);
    }

    private static DynamicTableFactory.Context createFactoryContext(
            ResolvedSchema schema, List<String> partitionKeys, Map<String, String> options) {
        return new FactoryUtil.DefaultDynamicTableContext(
                FactoryMocks.IDENTIFIER,
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock context",
                                partitionKeys,
                                options),
                        schema),
                Collections.emptyMap(),
                new Configuration(),
                FactoryMocks.class.getClassLoader(),
                false);
    }

    private static void testError(
            ResolvedSchema schema,
            BiConsumer<Map<String, String>, List<String>> mutations,
            String error) {
        final Map<String, String> options = getComplexOptions();
        final List<String> partitionKeys = new ArrayList<>();
        mutations.accept(options, partitionKeys);
        assertThatThrownBy(() -> createTableSource(schema, options, partitionKeys))
                .hasMessageContaining(error);
    }
}
