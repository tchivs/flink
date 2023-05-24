/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.converters;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroToFlinkSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
public class AvroToFlinkSchemaConverterTest {

    public static Stream<Arguments> typesToCheck() {
        return Stream.of(
                Arguments.of(SchemaBuilder.builder().doubleType(), new DoubleType(false)),
                Arguments.of(SchemaBuilder.builder().longType(), new BigIntType(false)),
                Arguments.of(SchemaBuilder.builder().intType(), new IntType(false)),
                Arguments.of(SchemaBuilder.builder().booleanType(), new BooleanType(false)),
                Arguments.of(SchemaBuilder.builder().floatType(), new FloatType(false)),
                Arguments.of(
                        SchemaBuilder.builder().bytesType(),
                        new VarBinaryType(false, VarBinaryType.MAX_LENGTH)),
                Arguments.of(
                        SchemaBuilder.builder().stringType(),
                        new VarCharType(false, VarCharType.MAX_LENGTH)),
                Arguments.of(
                        SchemaBuilder.builder().enumeration("color").symbols("red", "blue"),
                        new VarCharType(false, VarCharType.MAX_LENGTH)),
                Arguments.of(SchemaBuilder.builder().nullable().doubleType(), new DoubleType()),
                Arguments.of(SchemaBuilder.builder().nullable().longType(), new BigIntType()),
                Arguments.of(SchemaBuilder.builder().nullable().intType(), new IntType()),
                Arguments.of(SchemaBuilder.builder().nullable().booleanType(), new BooleanType()),
                Arguments.of(SchemaBuilder.builder().nullable().floatType(), new FloatType()),
                Arguments.of(
                        SchemaBuilder.builder().nullable().bytesType(),
                        new VarBinaryType(true, VarBinaryType.MAX_LENGTH)),
                Arguments.of(
                        SchemaBuilder.builder().nullable().stringType(),
                        new VarCharType(true, VarCharType.MAX_LENGTH)),
                Arguments.of(
                        SchemaBuilder.builder()
                                .nullable()
                                .enumeration("color")
                                .symbols("red", "blue"),
                        new VarCharType(true, VarCharType.MAX_LENGTH)));
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(Schema schema, LogicalType expected) {
        assertThat(AvroToFlinkSchemaConverter.toFlinkSchema(schema)).isEqualTo(expected);
    }

    @Test
    void testRecordWithCycles() {
        String avroSchemaString =
                "{\"type\": \"record\",\"name\": \"linked_list\",\"fields\" : "
                        + "[{\"name\": \"value\", \"type\": \"long\"},"
                        + "{\"name\": \"next\", \"type\": [\"null\", \"linked_list\"]}]}";
        final Schema schema = new Parser().parse(avroSchemaString);

        assertThatThrownBy(() -> AvroToFlinkSchemaConverter.toFlinkSchema(schema))
                .hasMessage("Cyclic schemas are not supported.");
    }
}
