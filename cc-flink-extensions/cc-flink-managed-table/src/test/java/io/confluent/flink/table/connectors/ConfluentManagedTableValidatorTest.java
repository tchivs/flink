/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.flink.table.connectors.ConfluentManagedTableValidator.validateAlterTableOptions;
import static io.confluent.flink.table.connectors.ConfluentManagedTableValidator.validateCreateTableOptions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link ConfluentManagedTableValidator}. */
@Confluent
public class ConfluentManagedTableValidatorTest {

    private static TableEnvironment tableEnv;

    @BeforeAll
    static void beforeAll() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    void testInvalidInternalConnectorOption() {
        testCreateTableError(
                "CREATE TABLE t (i INT) WITH ('connector' = 'datagen')",
                "Invalid value for option 'connector'.");
    }

    @Test
    void testInvalidInternalOptions() {
        testCreateTableError(
                "CREATE TABLE t (i INT) WITH ('kafka.bootstrap-servers' = 'localhost:8080')",
                "Unsupported options found for 't'.\n"
                        + "\n"
                        + "Unsupported options:\n"
                        + "\n"
                        + "kafka.bootstrap-servers\n"
                        + "\n"
                        + "Supported options:\n"
                        + "\n"
                        + "changelog.mode\n"
                        + "connector\n"
                        + "kafka.cleanup-policy\n"
                        + "kafka.max-message-size\n"
                        + "kafka.partitions\n"
                        + "kafka.retention.size\n"
                        + "kafka.retention.time\n"
                        + "key.fields-prefix\n"
                        + "key.format\n"
                        + "scan.bounded.mode\n"
                        + "scan.bounded.specific-offsets\n"
                        + "scan.bounded.timestamp-millis\n"
                        + "scan.startup.mode\n"
                        + "scan.startup.specific-offsets\n"
                        + "scan.startup.timestamp-millis\n"
                        + "value.fields-include\n"
                        + "value.format");
    }

    @Test
    void testInvalidInternalFormatOption() {
        testCreateTableError(
                "CREATE TABLE t (s STRING) WITH ('key.format' = 'raw', 'raw.invalid' = '42')",
                "Unsupported options found for 't'.\n"
                        + "\n"
                        + "Unsupported options:\n"
                        + "\n"
                        + "raw.invalid\n"
                        + "\n"
                        + "Supported options:\n"
                        + "\n"
                        + "changelog.mode\n"
                        + "connector\n"
                        + "kafka.cleanup-policy\n"
                        + "kafka.max-message-size\n"
                        + "kafka.partitions\n"
                        + "kafka.retention.size\n"
                        + "kafka.retention.time\n"
                        + "key.fields-prefix\n"
                        + "key.format\n"
                        // Format options appear
                        + "key.raw.charset\n"
                        + "key.raw.endianness\n"
                        + "scan.bounded.mode\n"
                        + "scan.bounded.specific-offsets\n"
                        + "scan.bounded.timestamp-millis\n"
                        + "scan.startup.mode\n"
                        + "scan.startup.specific-offsets\n"
                        + "scan.startup.timestamp-millis\n"
                        + "value.fields-include\n"
                        + "value.format");
    }

    @Test
    void testDefaultOptions() {
        assertThat(testCreateTableOptions("CREATE TABLE t (i INT, s STRING)"))
                .containsEntry("connector", "confluent")
                .containsEntry("changelog.mode", "retract")
                .containsEntry("kafka.cleanup-policy", "delete")
                .containsEntry("kafka.max-message-size", "2097164 bytes")
                .containsEntry("kafka.partitions", "6")
                .containsEntry("kafka.retention.size", "0 bytes")
                .containsEntry("kafka.retention.time", "604800000 ms")
                .containsEntry("scan.bounded.mode", "unbounded")
                .containsEntry("scan.startup.mode", "earliest-offset")
                .containsEntry("value.format", "avro-registry")
                // Unnecessary defaults are not shown
                .doesNotContainKey("value.fields-include");
    }

    @Test
    void testEnrichedOptions() {
        assertThat(
                        testCreateTableOptions(
                                "CREATE TABLE t (i INT, s STRING, PRIMARY KEY(i) NOT ENFORCED)"))
                .containsEntry("changelog.mode", "upsert")
                .containsEntry("key.format", "avro-registry");
    }

    @Test
    void testCustomOptions() {
        assertThat(
                        testCreateTableOptions(
                                "CREATE TABLE t (i INT, s STRING, PRIMARY KEY(i) NOT ENFORCED) "
                                        + "WITH ('changelog.mode' = 'append', 'key.format' = 'raw')"))
                .containsEntry("changelog.mode", "append")
                .containsEntry("key.format", "raw");
    }

    @Test
    void testImmutableOptions() {
        final Map<String, String> oldOptions = testCreateTableOptions("CREATE TABLE t (i INT)");
        final Map<String, String> newOptions = new HashMap<>(oldOptions);
        newOptions.put("kafka.partitions", "100");

        assertThatThrownBy(
                        () ->
                                validateAlterTableOptions(
                                        "t",
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList("i"),
                                        oldOptions,
                                        newOptions))
                .hasMessageContaining(
                        "Unsupported modification found for 't'.\n"
                                + "\n"
                                + "Unsupported options:\n"
                                + "\n"
                                + "kafka.partitions");
    }

    private void testCreateTableError(String sql, String error) {
        assertThatThrownBy(() -> testCreateTableOptions(sql)).hasMessageContaining(error);
    }

    private Map<String, String> testCreateTableOptions(String sql) {
        try {
            tableEnv.executeSql(sql);
            final ResolvedCatalogTable catalogTable =
                    (ResolvedCatalogTable)
                            tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                                    .orElseThrow(IllegalArgumentException::new)
                                    .getTable(new ObjectPath(tableEnv.getCurrentDatabase(), "t"));
            return validateCreateTableOptions(
                    "t",
                    catalogTable
                            .getResolvedSchema()
                            .getPrimaryKey()
                            .map(UniqueConstraint::getColumns)
                            .orElse(Collections.emptyList()),
                    catalogTable.getPartitionKeys(),
                    DataType.getFieldNames(
                            catalogTable.getResolvedSchema().toPhysicalRowDataType()),
                    catalogTable.getOptions());
        } catch (TableNotExistException e) {
            return fail(e);
        } finally {
            tableEnv.executeSql("DROP TABLE IF EXISTS t");
        }
    }
}
