/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.protobuf;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.flink.formats.converters.protobuf.CommonMappings.TypeMapping;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkToProtoSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
class FlinkToProtoSchemaConverterTest {

    public static final String PACKAGE_NAME = "io.confluent.protobuf.generated";

    public static Stream<Arguments> typesToCheck() {
        return CommonMappings.get().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(TypeMapping mapping) {
        final Descriptor descriptor =
                FlinkToProtoSchemaConverter.fromFlinkSchema(
                        (RowType) mapping.getFlinkType(), "Row", PACKAGE_NAME);

        // clear any whitespace only lines
        assertThat(new ProtobufSchema(descriptor).toString().trim().replaceAll("\\s*\n", "\n"))
                .isEqualTo(mapping.getExpectedString().trim().replaceAll("\\s*\n", "\n"));
    }
}
