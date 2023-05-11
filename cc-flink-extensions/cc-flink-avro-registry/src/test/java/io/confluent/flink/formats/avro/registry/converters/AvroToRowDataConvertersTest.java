/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry.converters;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.avro.registry.converters.AvroToRowDataConverters.AvroToRowDataConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowDataToAvroConverters}. */
@ExtendWith(TestLoggerExtension.class)
class AvroToRowDataConvertersTest {

    @Test
    void testTimestamps() {
        final Schema schema =
                SchemaBuilder.builder()
                        .record("topLevelRow")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("timestamp_micros")
                        .type(
                                LogicalTypes.timestampMicros()
                                        .addToSchema(SchemaBuilder.builder().longType()))
                        .noDefault()
                        .name("timestamp_millis")
                        .type(
                                LogicalTypes.timestampMillis()
                                        .addToSchema(SchemaBuilder.builder().longType()))
                        .noDefault()
                        .name("local_timestamp_micros")
                        .type(
                                LogicalTypes.localTimestampMicros()
                                        .addToSchema(SchemaBuilder.builder().longType()))
                        .noDefault()
                        .name("local_timestamp_millis")
                        .type(
                                LogicalTypes.localTimestampMillis()
                                        .addToSchema(SchemaBuilder.builder().longType()))
                        .noDefault()
                        .endRecord();

        final LogicalType flinkSchema = AvroToFlinkSchemaConverter.toFlinkSchema(schema);
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createConverter(schema, flinkSchema);

        final GenericRowData expected = new GenericRowData(4);
        expected.setField(0, TimestampData.fromEpochMillis(1, 111_000));
        expected.setField(1, TimestampData.fromEpochMillis(1));
        expected.setField(2, TimestampData.fromEpochMillis(1, 111_000));
        expected.setField(3, TimestampData.fromEpochMillis(1));
        assertThat(
                        converter.convert(
                                new GenericRecordBuilder(schema)
                                        .set("timestamp_micros", 1111L)
                                        .set("timestamp_millis", 1L)
                                        .set("local_timestamp_micros", 1111L)
                                        .set("local_timestamp_millis", 1L)
                                        .build()))
                .isEqualTo(expected);
    }

    @Test
    void testCustomMap() {
        final Schema fixedSchema = SchemaBuilder.fixed("test").size(2);
        final Schema mapEntrySchema =
                SchemaBuilder.record("MapEntry")
                        .namespace("io.confluent.connect.avro")
                        .fields()
                        .name("key")
                        .type(LogicalTypes.decimal(4, 2).addToSchema(fixedSchema))
                        .noDefault()
                        .name("value")
                        .type()
                        .floatBuilder()
                        .endFloat()
                        .noDefault()
                        .endRecord();
        final Schema schema =
                SchemaBuilder.builder()
                        .record("top")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("custom_map")
                        .type()
                        .array()
                        .items(mapEntrySchema)
                        .noDefault()
                        .endRecord();

        final LogicalType flinkSchema = AvroToFlinkSchemaConverter.toFlinkSchema(schema);
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createConverter(schema, flinkSchema);

        final GenericRowData expected = new GenericRowData(1);
        final Map<DecimalData, Float> customMap = new HashMap<>();
        final DecimalData decimalData = DecimalData.fromBigDecimal(new BigDecimal("12.34"), 4, 2);
        customMap.put(decimalData, 123f);
        expected.setField(0, new GenericMapData(customMap));
        final Object actual =
                converter.convert(
                        new GenericRecordBuilder(schema)
                                .set(
                                        "custom_map",
                                        Collections.singletonList(
                                                new GenericRecordBuilder(mapEntrySchema)
                                                        .set(
                                                                "key",
                                                                new GenericData.Fixed(
                                                                        fixedSchema,
                                                                        decimalData
                                                                                .toUnscaledBytes()))
                                                        .set("value", 123f)
                                                        .build()))
                                .build());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testEnum() {
        final Schema enumSchema = SchemaBuilder.enumeration("colors").symbols("red", "blue");
        final Schema schema =
                SchemaBuilder.builder()
                        .record("top")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("enum_field")
                        .type(enumSchema)
                        .noDefault()
                        .endRecord();

        final LogicalType flinkSchema = AvroToFlinkSchemaConverter.toFlinkSchema(schema);
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createConverter(schema, flinkSchema);

        final GenericRowData expected = new GenericRowData(1);
        expected.setField(0, StringData.fromString("red"));
        final Object actual =
                converter.convert(
                        new GenericRecordBuilder(schema)
                                .set("enum_field", new GenericData.EnumSymbol(enumSchema, "red"))
                                .build());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testNull() {
        final Schema schema =
                SchemaBuilder.builder()
                        .record("top")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("null_field")
                        .type()
                        .nullable()
                        .intType()
                        .noDefault()
                        .endRecord();

        final LogicalType flinkSchema = AvroToFlinkSchemaConverter.toFlinkSchema(schema);
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createConverter(schema, flinkSchema);

        final GenericRowData expected = new GenericRowData(1);

        final Object actual =
                converter.convert(new GenericRecordBuilder(schema).set("null_field", null).build());
        assertThat(actual).isEqualTo(expected);
    }
}
