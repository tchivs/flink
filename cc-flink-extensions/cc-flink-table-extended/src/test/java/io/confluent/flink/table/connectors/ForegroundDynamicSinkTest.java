/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForegroundDynamicTableFactory} and {@link ForegroundDynamicSink}. */
public class ForegroundDynamicSinkTest {

    @Test
    public void testTableFactory() {
        final DynamicTableSink expectedSink =
                new ForegroundDynamicSink(
                        MemorySize.ofMebiBytes(8),
                        Duration.ofSeconds(30),
                        DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.INT()),
                                        DataTypes.FIELD("b", DataTypes.STRING()))
                                .notNull());

        final ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("a", DataTypes.INT()),
                        Column.physical("b", DataTypes.STRING()));

        final Map<String, String> options = new HashMap<>();
        options.put("connector", "cc-foreground");
        options.put("batch-size-max", "8mb");
        options.put("socket-timeout", "30s");

        final DynamicTableSink actualSink =
                FactoryUtil.createDynamicTableSink(
                        null,
                        ObjectIdentifier.of("cat", "db", "table"),
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                                        null,
                                        Collections.emptyList(),
                                        options),
                                schema),
                        Collections.emptyMap(),
                        new Configuration(),
                        ForegroundDynamicSinkTest.class.getClassLoader(),
                        true);

        assertThat(actualSink).isEqualTo(expectedSink);
    }
}
