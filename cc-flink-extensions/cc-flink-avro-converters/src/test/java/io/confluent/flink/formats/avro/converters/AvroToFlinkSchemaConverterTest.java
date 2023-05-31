/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.converters;

import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.avro.converters.CommonMappings.TypeMapping;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.confluent.flink.formats.avro.converters.CommonMappings.nullable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroToFlinkSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
public class AvroToFlinkSchemaConverterTest {

    public static Stream<Arguments> typesToCheck() {
        return Stream.concat(
                        CommonMappings.get(),
                        Stream.of(
                                new TypeMapping(
                                        SchemaBuilder.builder()
                                                .enumeration("color")
                                                .symbols("red", "blue"),
                                        new VarCharType(false, VarCharType.MAX_LENGTH)),
                                new TypeMapping(
                                        nullable(
                                                SchemaBuilder.builder()
                                                        .enumeration("color")
                                                        .symbols("red", "blue")),
                                        new VarCharType(true, VarCharType.MAX_LENGTH))))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(TypeMapping mapping) {
        assertThat(AvroToFlinkSchemaConverter.toFlinkSchema(mapping.getAvroSchema()))
                .isEqualTo(mapping.getFlinkType());
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
