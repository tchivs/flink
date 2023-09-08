/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.CollectionUtil.entry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConfluentManagedTableSource} and {@link ConfluentManagedTableSink}. */
@Confluent
public class ConfluentManagedTableITCase extends ConfluentManagedTableTestBase {

    @Nested
    class RetractMode {

        @Test
        void testMetadata() throws Exception {
            final String tableSchema =
                    "(\n"
                            + "  computed_discounted AS physical_price * 0.8,\n"
                            + "  metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "  physical_price INT,\n"
                            + "  physical_name STRING,\n"
                            + "  metadata_header MAP<STRING, BYTES> METADATA FROM 'headers',\n"
                            + "  physical_currency STRING NOT NULL,\n"
                            + "  $rowtime TIMESTAMP_LTZ(3) NOT NULL METADATA VIRTUAL\n"
                            + ")";
            final String sourceTopic = createTopic("source");
            createTable(sourceTopic, "source", tableSchema, entry("changelog.mode", "retract"));
            final String sinkTopic = createTopic("sink");
            createTable(sinkTopic, "sink", tableSchema, entry("changelog.mode", "retract"));

            tableEnv.executeSql(
                            "INSERT INTO source VALUES\n"
                                    + "(TO_TIMESTAMP_LTZ(1, 3), 1, 'Milk', MAP['k1', X'C0FFEE'], 'EUR'),\n"
                                    + "(TO_TIMESTAMP_LTZ(2, 3), 3, 'Rice', CAST(NULL AS MAP<STRING, BYTES>), 'USD'),\n"
                                    + "(TO_TIMESTAMP_LTZ(3, 3), 5, 'Eggs', CAST(NULL AS MAP<STRING, BYTES>), 'USD'),\n"
                                    + "(TO_TIMESTAMP_LTZ(4, 3), 2, 'Nuts', CAST(NULL AS MAP<STRING, BYTES>), 'USD'),\n"
                                    // timestamp 100 is out of range for bounded mode
                                    + "(TO_TIMESTAMP_LTZ(100, 3), 0, 'Invalid', CAST(NULL AS MAP<STRING, BYTES>), 'USD')\n")
                    .await();

            tableEnv.executeSql(
                            "INSERT INTO sink \n"
                                    + "SELECT $rowtime, physical_price, physical_name, metadata_header, physical_currency\n"
                                    + "FROM source")
                    .await();

            final String testSchema =
                    "(\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "metadata_header MAP<STRING, BYTES> METADATA FROM 'headers',\n"
                            + "metadata_partition INT METADATA FROM 'partition',\n"
                            + "physical_price INT,\n"
                            + "physical_name STRING NOT NULL,\n"
                            + "physical_currency STRING\n"
                            + ")";
            createTable(sinkTopic, "test", testSchema, entry("changelog.mode", "append"));

            // Properties:
            // - metadata is passed end-to-end
            // - "op" in header is not visible
            // - partitioning is based on entire value
            //   (i.e. USD is split across partitions but retractions are correct)
            testPartitions(
                    "test",
                    Arrays.asList(
                            "+I[1970-01-01T00:00:00.001Z, {k1=[-64, -1, -18]}, 0, 1, Milk, EUR]",
                            "+I[1970-01-01T00:00:00.003Z, {}, 0, 5, Eggs, USD]",
                            "+I[1970-01-01T00:00:00.004Z, {}, 0, 2, Nuts, USD]"),
                    Collections.emptyList(),
                    Collections.singletonList("+I[1970-01-01T00:00:00.002Z, {}, 2, 3, Rice, USD]"),
                    Collections.emptyList());

            deleteTopic(sourceTopic);
            deleteTopic(sinkTopic);
        }

        @Test
        void testRoundTrip() throws Exception {
            final String tableSchema =
                    "(\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "physical_name STRING,\n"
                            + "physical_sum INT\n"
                            + ")";
            final String tableTopic = createTopic("t");
            createTable(tableTopic, "t", tableSchema, entry("changelog.mode", "retract"));

            insertRows(
                    "t",
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(1), "EUR", 1),
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(2), "USD", 3),
                    Row.ofKind(RowKind.UPDATE_BEFORE, Instant.ofEpochMilli(2), "USD", 3),
                    Row.ofKind(RowKind.UPDATE_AFTER, Instant.ofEpochMilli(3), "USD", 10),
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(100), "EOS", -1));

            final List<String> materializedRows = new ArrayList<>();
            tableEnv.from("t")
                    .execute()
                    .collect()
                    .forEachRemaining(
                            row -> {
                                final RowKind kind = row.getKind();
                                row.setKind(RowKind.INSERT);
                                switch (kind) {
                                    case INSERT:
                                    case UPDATE_AFTER:
                                        materializedRows.add(row.toString());
                                        break;
                                    case UPDATE_BEFORE:
                                    case DELETE:
                                        materializedRows.remove(row.toString());
                                        break;
                                }
                            });

            // Properties:
            // - retractions remain correct during round trip
            assertThat(materializedRows)
                    .containsExactlyInAnyOrder(
                            "+I[1970-01-01T00:00:00.001Z, EUR, 1]",
                            "+I[1970-01-01T00:00:00.003Z, USD, 10]");

            deleteTopic(tableTopic);
        }

        @Test
        void testPartitionedBy() throws Exception {
            final String tableSchema =
                    "(\n"
                            + "metadata_partition INT METADATA FROM 'partition' VIRTUAL,\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "physical_name STRING,\n"
                            + "physical_sum INT\n"
                            + ")\n"
                            + "PARTITIONED BY (physical_name)";
            final String tableTopic = createTopic("t");
            createTable(
                    tableTopic,
                    "t",
                    tableSchema,
                    entry("changelog.mode", "retract"),
                    entry("key.format", "csv"));

            insertRows(
                    "t",
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(1), "EUR", 1),
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(2), "USD", 3),
                    Row.ofKind(RowKind.UPDATE_BEFORE, Instant.ofEpochMilli(2), "USD", 3),
                    Row.ofKind(RowKind.UPDATE_AFTER, Instant.ofEpochMilli(3), "USD", 10),
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(100), "EOS", -1));

            // Properties:
            // - all USD rows end up in same partition
            // - retractions remain correct within partition
            testPartitions(
                    "t",
                    Collections.singletonList("+I[0, 1970-01-01T00:00:00.001Z, EUR, 1]"),
                    Collections.emptyList(),
                    Arrays.asList(
                            "+I[2, 1970-01-01T00:00:00.002Z, USD, 3]",
                            "-U[2, 1970-01-01T00:00:00.002Z, USD, 3]",
                            "+U[2, 1970-01-01T00:00:00.003Z, USD, 10]"),
                    Collections.emptyList());

            deleteTopic(tableTopic);
        }
    }

    @Nested
    class UpsertMode {

        @Test
        void testRoundTrip() throws Exception {
            final String tableSchema =
                    "(\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "physical_name STRING,\n"
                            + "physical_sum INT,\n"
                            + "PRIMARY KEY(physical_name) NOT ENFORCED\n"
                            + ")";
            final String sourceTopic = createTopic("source");
            createTable(
                    sourceTopic,
                    "source",
                    tableSchema,
                    entry("changelog.mode", "upsert"),
                    entry("key.format", "csv"));

            insertRows(
                    "source",
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(1), "EUR", 1),
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(2), "USD", 3),
                    Row.ofKind(RowKind.UPDATE_AFTER, Instant.ofEpochMilli(3), "USD", 10),
                    Row.ofKind(RowKind.UPDATE_AFTER, Instant.ofEpochMilli(3), "USD", 100),
                    Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(100), "EOS", -1));

            final String sinkTopic = createTopic("sink");
            createTable(
                    sinkTopic,
                    "sink",
                    tableSchema,
                    entry("changelog.mode", "upsert"),
                    entry("key.format", "csv"));

            tableEnv.executeSql(
                            "INSERT INTO sink "
                                    + "SELECT TO_TIMESTAMP_LTZ(0, 3), physical_name, CAST(SUM(physical_sum) AS INT)"
                                    + "FROM source "
                                    + "GROUP BY physical_name")
                    .await();

            final String testSchema =
                    "(\n"
                            + "metadata_partition INT METADATA FROM 'partition' VIRTUAL,\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "physical_value STRING\n"
                            + ")";
            createTable(
                    sinkTopic,
                    "test",
                    testSchema,
                    entry("changelog.mode", "append"),
                    entry("value.format", "raw"));

            // Properties:
            // - all USD rows end up in same partition
            // - upserts remain correct within partition
            // - deletion is modelled as NULL tombstones
            testPartitions(
                    "test",
                    Collections.singletonList("+I[0, 1970-01-01T00:00:00Z, 1]"),
                    Collections.emptyList(),
                    Arrays.asList(
                            "+I[2, 1970-01-01T00:00:00Z, 3]",
                            "+I[2, 1970-01-01T00:00:00Z, null]",
                            "+I[2, 1970-01-01T00:00:00Z, 10]",
                            "+I[2, 1970-01-01T00:00:00Z, null]",
                            "+I[2, 1970-01-01T00:00:00Z, 100]"),
                    Collections.emptyList());

            deleteTopic(sourceTopic);
            deleteTopic(sinkTopic);
        }
    }

    @Nested
    class AppendMode {

        @Test
        void testRoundRobin() throws Exception {
            final String tableSchema =
                    "(\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "physical_name STRING,\n"
                            + "physical_price INT\n"
                            + ")";
            final String tableTopic = createTopic("t");
            createTable(tableTopic, "t", tableSchema, entry("changelog.mode", "append"));

            tableEnv.executeSql(
                            "INSERT INTO t VALUES\n"
                                    + "  (TO_TIMESTAMP_LTZ(1, 3), 'Eggs', 12),\n"
                                    + "  (TO_TIMESTAMP_LTZ(2, 3), 'Milk', 2),\n"
                                    + "  (TO_TIMESTAMP_LTZ(3, 3), 'Nuts', 5)")
                    .await();

            final List<String> rows = new ArrayList<>();
            tableEnv.from("t").execute().collect().forEachRemaining(r -> rows.add(r.toString()));

            // Properties:
            // - we can only test here that no data is lost
            assertThat(rows)
                    .containsExactlyInAnyOrder(
                            "+I[1970-01-01T00:00:00.002Z, Milk, 2]",
                            "+I[1970-01-01T00:00:00.003Z, Nuts, 5]",
                            "+I[1970-01-01T00:00:00.001Z, Eggs, 12]");

            deleteTopic(tableTopic);
        }

        @Test
        void testPartitionedBy() throws Exception {
            final String tableSchema =
                    "(\n"
                            + "metadata_partition INT METADATA FROM 'partition' VIRTUAL,\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "physical_name STRING,\n"
                            + "physical_price INT\n"
                            + ")\n"
                            + "PARTITIONED BY (physical_name)";
            final String tableTopic = createTopic("t");
            createTable(
                    tableTopic,
                    "t",
                    tableSchema,
                    entry("changelog.mode", "append"),
                    entry("key.format", "csv"));

            tableEnv.executeSql(
                            "INSERT INTO t VALUES\n"
                                    + "  (TO_TIMESTAMP_LTZ(1, 3), 'Eggs', 12),\n"
                                    + "  (TO_TIMESTAMP_LTZ(2, 3), 'Milk', 2),\n"
                                    + "  (TO_TIMESTAMP_LTZ(3, 3), 'Nuts', 5),\n"
                                    + "  (TO_TIMESTAMP_LTZ(4, 3), 'Nuts', 10),\n"
                                    + "  (TO_TIMESTAMP_LTZ(5, 3), 'Milk', 20)")
                    .await();

            // Properties:
            // - no round-robin anymore
            testPartitions(
                    "t",
                    Collections.emptyList(),
                    Arrays.asList(
                            "+I[1, 1970-01-01T00:00:00.003Z, Nuts, 5]",
                            "+I[1, 1970-01-01T00:00:00.004Z, Nuts, 10]"),
                    Collections.singletonList("+I[2, 1970-01-01T00:00:00.001Z, Eggs, 12]"),
                    Arrays.asList(
                            "+I[3, 1970-01-01T00:00:00.002Z, Milk, 2]",
                            "+I[3, 1970-01-01T00:00:00.005Z, Milk, 20]"));

            deleteTopic(tableTopic);
        }

        @Test
        void testReadRetract() throws Exception {
            final String tableSchema =
                    "(\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "physical_name STRING,\n"
                            + "physical_sum INT\n"
                            + ")";
            final String tableTopic = createTopic("t");
            createTable(tableTopic, "t", tableSchema, entry("changelog.mode", "retract"));

            tableEnv.executeSql(
                            "INSERT INTO t\n"
                                    + "SELECT MAX(ts), name, CAST(SUM(c) AS INT) FROM (\n"
                                    + "VALUES\n"
                                    + "  (TO_TIMESTAMP_LTZ(0, 3), 'Eggs', 3),\n"
                                    + "  (TO_TIMESTAMP_LTZ(1, 3), 'Eggs', 2),\n"
                                    + "  (TO_TIMESTAMP_LTZ(2, 3), 'Nuts', 5),\n"
                                    + "  (TO_TIMESTAMP_LTZ(4, 3), 'Eggs', 10),\n"
                                    + "  (TO_TIMESTAMP_LTZ(100, 3), 'Milk', 2)\n"
                                    + ") AS Products(ts, name, c) GROUP BY name")
                    .await();

            final String testSchema =
                    "(\n"
                            + "metadata_header MAP<STRING, BYTES> METADATA FROM 'headers',\n"
                            + "physical_name STRING,\n"
                            + "physical_sum INT\n"
                            + ")";
            createTable(tableTopic, "test", testSchema, entry("changelog.mode", "append"));

            final List<String> materializedRows = new ArrayList<>();
            tableEnv.from("test")
                    .execute()
                    .collect()
                    .forEachRemaining(
                            row -> {
                                final Map<String, byte[]> header =
                                        row.getFieldAs("metadata_header");
                                final RowKind kind =
                                        RowKind.fromByteValue(
                                                header.containsKey("op") ? header.get("op")[0] : 0);
                                header.remove("op");
                                switch (kind) {
                                    case INSERT:
                                    case UPDATE_AFTER:
                                        materializedRows.add(row.toString());
                                        break;
                                    case UPDATE_BEFORE:
                                    case DELETE:
                                        materializedRows.remove(row.toString());
                                        break;
                                }
                            });

            // Properties:
            // - append mode can read header correctly
            assertThat(materializedRows)
                    .containsExactlyInAnyOrder("+I[{}, Nuts, 5]", "+I[{}, Eggs, 15]");

            deleteTopic(tableTopic);
        }

        @Test
        void testStaticPartitions() throws Exception {
            final String tableSchema =
                    "(\n"
                            + "metadata_partition INT METADATA FROM 'partition' VIRTUAL,\n"
                            + "metadata_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n"
                            + "physical_name STRING,\n"
                            + "physical_sum INT\n"
                            + ") PARTITIONED BY (physical_name)";
            final String tableTopic = createTopic("t");
            createTable(
                    tableTopic,
                    "t",
                    tableSchema,
                    entry("changelog.mode", "append"),
                    entry("key.format", "csv"));

            tableEnv.executeSql(
                            "INSERT INTO t PARTITION (physical_name='Milk') "
                                    + "VALUES (TO_TIMESTAMP_LTZ(0, 3), 1), (TO_TIMESTAMP_LTZ(1, 3), 2)")
                    .await();

            // Properties:
            // - no round-robin anymore
            // - less verbosity due to static column definition
            testPartitions(
                    "t",
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Arrays.asList(
                            "+I[3, 1970-01-01T00:00:00Z, Milk, 1]",
                            "+I[3, 1970-01-01T00:00:00.001Z, Milk, 2]"));

            deleteTopic(tableTopic);
        }
    }

    // --------------------------------------------------------------------------------------------

    private void testPartitions(
            String testTable,
            List<String> expectedPartition0,
            List<String> expectedPartition1,
            List<String> expectedPartition2,
            List<String> expectedPartition3) {
        final List<Row> results = new ArrayList<>();
        tableEnv.from(testTable).execute().collect().forEachRemaining(results::add);
        testPartition(results, expectedPartition0, 0);
        testPartition(results, expectedPartition1, 1);
        testPartition(results, expectedPartition2, 2);
        testPartition(results, expectedPartition3, 3);
    }

    private void testPartition(List<Row> actual, List<String> expected, int partitionId) {
        final List<String> actualPartition =
                actual.stream()
                        .filter(r -> Objects.equals(r.getField("metadata_partition"), partitionId))
                        .map(Row::toString)
                        .collect(Collectors.toList());
        assertThat(actualPartition).containsExactlyInAnyOrderElementsOf(expected);
    }

    @SuppressWarnings("rawtypes")
    private void createTable(
            String topicName, String tableName, String schema, Map.Entry... options) {
        final StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ");
        builder.append(tableName);
        builder.append(" ");
        builder.append(schema);
        builder.append(" WITH (");

        final Map<String, String> allOptions = new HashMap<>();
        allOptions.put("connector", "confluent");
        allOptions.put("scan.startup.mode", "earliest-offset");
        allOptions.put("scan.bounded.mode", "timestamp");
        allOptions.put("scan.bounded.timestamp-millis", "100");
        allOptions.put("confluent.kafka.topic", topicName);
        allOptions.put("confluent.kafka.bootstrap-servers", getBootstrapServers());
        allOptions.put("confluent.kafka.logical-cluster-id", "lkc-4242");
        allOptions.put("confluent.kafka.credentials-source", "properties");
        allOptions.put("value.format", "csv");
        Stream.of(options)
                .forEach(e -> allOptions.put(e.getKey().toString(), e.getValue().toString()));

        builder.append(
                allOptions.entrySet().stream()
                        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",\n")));

        builder.append(")");

        final String sql = builder.toString();
        tableEnv.executeSql(sql);
    }

    private void insertRows(String table, Row... rows) throws Exception {
        final DataStream<Row> elements =
                env.fromCollection(
                        Arrays.asList(rows),
                        Types.ROW_NAMED(
                                new String[] {"metadata_ts", "physical_name", "physical_sum"},
                                Types.INSTANT,
                                Types.STRING,
                                Types.INT));

        tableEnv.fromChangelogStream(
                        elements,
                        Schema.newBuilder()
                                .column("metadata_ts", "TIMESTAMP_LTZ(3)")
                                .column("physical_name", "STRING")
                                .column("physical_sum", "INT")
                                .build())
                .insertInto(table)
                .execute()
                .await();
    }
}
