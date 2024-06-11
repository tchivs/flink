/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.avro;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.converters.avro.CommonMappings.TypeMapping;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkToAvroSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
class FlinkToAvroSchemaConverterTest {

    public static Stream<Arguments> typesToCheck() {
        return CommonMappings.get().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(TypeMapping mapping) {
        assertThat(
                        FlinkToAvroSchemaConverter.fromFlinkSchema(
                                mapping.getFlinkType(), "io.confluent.row"))
                .isEqualTo(mapping.getAvroSchema());
    }

    @Test
    void testInvalidFieldNames() {
        final RowType rowType =
                new RowType(
                        false,
                        Collections.singletonList(
                                new RowField(
                                        "digital.hema.transport.v2.HomeDeliveryAddress",
                                        new BigIntType(false))));

        assertThatThrownBy(
                        () ->
                                FlinkToAvroSchemaConverter.fromFlinkSchema(
                                        rowType, "io.confluent.row"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Illegal field name for AVRO format "
                                + "`digital.hema.transport.v2.HomeDeliveryAddress`. AVRO expects field"
                                + " names to start with [A-Za-z_] subsequently contain only [A-Za-z0-9_].");
    }
}
