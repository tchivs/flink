/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForegroundResultTableFactory} and {@link ForegroundResultTableSink}. */
@Confluent
class ForegroundResultTableSinkTest {

    @Test
    void testTableFactory() {
        final DynamicTableSink expectedSink =
                new ForegroundResultTableSink(
                        MemorySize.ofMebiBytes(8),
                        Duration.ofSeconds(30),
                        DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.INT()),
                                        DataTypes.FIELD("b", DataTypes.STRING()))
                                .notNull(),
                        ZoneId.of("UTC"));

        final ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("a", DataTypes.INT()),
                        Column.physical("b", DataTypes.STRING()));

        final Map<String, String> options = new HashMap<>();
        options.put("connector", "cc-foreground-sink");

        final Configuration configuration = new Configuration();
        configuration.set(ForegroundResultTableFactory.MAX_BATCH_SIZE, MemorySize.ofMebiBytes(8));
        configuration.set(ForegroundResultTableFactory.SOCKET_TIMEOUT, Duration.ofSeconds(30));
        configuration.set(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");

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
                        configuration,
                        ForegroundResultTableSinkTest.class.getClassLoader(),
                        true);

        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    void testMaxParallelismIsSetToOne() {
        final DynamicTableSink tableSink =
                new ForegroundResultTableSink(
                        MemorySize.ofMebiBytes(8),
                        Duration.ofSeconds(30),
                        DataTypes.ROW(DataTypes.FIELD("foo", DataTypes.STRING())).notNull(),
                        ZoneId.of("UTC"));

        final DataStreamSinkProvider sinkProvider =
                (DataStreamSinkProvider)
                        tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSink<?> generated =
                sinkProvider.consumeDataStream(
                        name -> Optional.of("generated"), env.fromElements(new BinaryRowData(1)));
        assertThat(generated.getTransformation().getUid()).isEqualTo("generated");
        assertThat(generated.getTransformation().getMaxParallelism()).isEqualTo(1);
    }
}
