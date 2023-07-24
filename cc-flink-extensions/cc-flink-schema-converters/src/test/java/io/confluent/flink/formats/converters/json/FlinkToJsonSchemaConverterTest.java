/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.converters.json.CommonMappings.TypeMapping;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkToJsonSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
class FlinkToJsonSchemaConverterTest {

    public static Stream<Arguments> typesToCheck() {
        return CommonMappings.get().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(TypeMapping mapping) {
        assertThat(
                        FlinkToJsonSchemaConverter.fromFlinkSchema(
                                mapping.getFlinkType(), "io.confluent.row"))
                .isEqualTo(mapping.getJsonSchema());
    }
}
