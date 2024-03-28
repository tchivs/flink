/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.avro.converters;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.converters.avro.AvroToFlinkSchemaConverter;
import io.confluent.flink.formats.converters.avro.CommonMappings;
import io.confluent.flink.formats.converters.avro.CommonMappings.TypeMapping;
import io.confluent.flink.formats.converters.avro.CommonMappings.TypeMappingWithData;
import io.confluent.flink.formats.converters.avro.util.UnionUtil;
import io.confluent.flink.formats.registry.avro.converters.AvroToRowDataConverters.AvroToRowDataConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link io.confluent.flink.formats.registry.avro.converters.AvroToRowDataConverters}.
 */
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
    void testMultiset() {
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
                        .intType()
                        .noDefault()
                        .endRecord();
        final Schema schema =
                SchemaBuilder.builder()
                        .record("top")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("custom_multiset")
                        .type()
                        .array()
                        .items(mapEntrySchema)
                        .noDefault()
                        .name("string_multiset")
                        .type()
                        .map()
                        .values()
                        .intType()
                        .noDefault()
                        .endRecord();

        final LogicalType flinkSchema =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField(
                                        "custom_multiset",
                                        new MultisetType(false, new DecimalType(false, 4, 2))),
                                new RowField(
                                        "string_multiset",
                                        new MultisetType(
                                                false,
                                                new VarCharType(false, VarCharType.MAX_LENGTH)))));
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createConverter(schema, flinkSchema);

        final GenericRowData expected = new GenericRowData(2);
        final Map<DecimalData, Integer> customMap = new HashMap<>();
        final DecimalData decimalData = DecimalData.fromBigDecimal(new BigDecimal("12.34"), 4, 2);
        customMap.put(decimalData, 12);
        expected.setField(0, new GenericMapData(customMap));
        final Map<StringData, Integer> stringMap = new HashMap<>();
        final StringData stringData = StringData.fromString("ABCDEF");
        stringMap.put(stringData, 22);
        expected.setField(1, new GenericMapData(stringMap));
        final Map<String, Integer> stringMultiset = new HashMap<>();
        stringMultiset.put("ABCDEF", 22);
        final Object actual =
                converter.convert(
                        new GenericRecordBuilder(schema)
                                .set(
                                        "custom_multiset",
                                        Collections.singletonList(
                                                new GenericRecordBuilder(mapEntrySchema)
                                                        .set(
                                                                "key",
                                                                new GenericData.Fixed(
                                                                        fixedSchema,
                                                                        decimalData
                                                                                .toUnscaledBytes()))
                                                        .set("value", 12)
                                                        .build()))
                                .set("string_multiset", stringMultiset)
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

    @Test
    void testBytes() {
        final Schema fixed = SchemaBuilder.fixed("fixed").size(3);
        final Schema fixedDecimal =
                LogicalTypes.decimal(5, 3).addToSchema(SchemaBuilder.fixed("fixedDecimal").size(5));
        final Schema decimal =
                LogicalTypes.decimal(5, 3).addToSchema(SchemaBuilder.builder().bytesType());
        final Schema schema =
                SchemaBuilder.builder()
                        .record("top")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("binary")
                        .type()
                        .bytesType()
                        .noDefault()
                        .name("fixed_binary")
                        .type(fixed)
                        .noDefault()
                        .name("fixed_decimal")
                        .type(fixedDecimal)
                        .noDefault()
                        .name("decimal")
                        .type(decimal)
                        .noDefault()
                        .endRecord();

        final LogicalType flinkSchema = AvroToFlinkSchemaConverter.toFlinkSchema(schema);
        final AvroToRowDataConverter converter =
                AvroToRowDataConverters.createConverter(schema, flinkSchema);

        final byte[] expectedBytes = {1, 2, 3};
        final BigDecimal bigDecimal = new BigDecimal("12.123");
        final GenericRowData expected = new GenericRowData(4);
        expected.setField(0, expectedBytes);
        expected.setField(1, expectedBytes);
        expected.setField(2, DecimalData.fromBigDecimal(bigDecimal, 5, 3));
        expected.setField(3, DecimalData.fromBigDecimal(bigDecimal, 5, 3));

        final Object actual =
                converter.convert(
                        new GenericRecordBuilder(schema)
                                .set("binary", ByteBuffer.wrap(expectedBytes))
                                .set("fixed_binary", new GenericData.Fixed(fixed, expectedBytes))
                                .set(
                                        "fixed_decimal",
                                        new GenericData.Fixed(
                                                fixed, bigDecimal.unscaledValue().toByteArray()))
                                .set(
                                        "decimal",
                                        ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray()))
                                .build());
        assertThat(actual).isEqualTo(expected);
    }

    public static Stream<Arguments> unionTests() {
        final List<TypeMapping> singleTypeMappings =
                Stream.concat(
                                CommonMappings.getNotNull().stream(),
                                Stream.of(
                                        new TypeMapping(
                                                SchemaBuilder.builder()
                                                        .enumeration("colors")
                                                        .symbols("red", "blue"),
                                                new VarCharType(false, VarCharType.MAX_LENGTH))))
                        .collect(Collectors.toList());
        return UnionUtil.createUnionTypeMappings(singleTypeMappings)
                .map(UnionUtil::withData)
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("unionTests")
    void testUnion(TypeMappingWithData mapping) {
        AvroToRowDataConverter converter =
                AvroToRowDataConverters.createConverter(
                        mapping.getAvroSchema(), mapping.getFlinkType());

        assertThat(mapping.getFlinkData()).isEqualTo(converter.convert(mapping.getAvroData()));
    }
}
