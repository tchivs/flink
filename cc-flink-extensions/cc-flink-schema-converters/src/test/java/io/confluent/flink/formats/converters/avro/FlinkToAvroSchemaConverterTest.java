/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.avro;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.converters.avro.CommonMappings.TypeMapping;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkToAvroSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
class FlinkToAvroSchemaConverterTest {

    public static Stream<Arguments> typesToCheck() {
        return Stream.concat(
                        CommonMappings.get(),
                        Stream.of(
                                new TypeMapping(
                                        SchemaBuilder.map().values().intType(),
                                        new MultisetType(
                                                false,
                                                new VarCharType(false, VarCharType.MAX_LENGTH))),
                                new TypeMapping(
                                        SchemaBuilder.array()
                                                .items(
                                                        CommonMappings.connectCustomMapType(
                                                                SchemaBuilder.builder().longType(),
                                                                SchemaBuilder.builder().intType())),
                                        new MultisetType(false, new BigIntType(false)))))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(TypeMapping mapping) {
        assertThat(
                        FlinkToAvroSchemaConverter.fromFlinkSchema(
                                mapping.getFlinkType(), "io.confluent.row"))
                .isEqualTo(mapping.getAvroSchema());
    }
}
