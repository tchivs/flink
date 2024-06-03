/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.protobuf;

import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.flink.formats.converters.protobuf.CommonMappings.TypeMapping;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkToProtoSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
class FlinkToProtoSchemaConverterTest {

    public static final String PACKAGE_NAME = "io.confluent.protobuf.generated";

    public static Stream<Arguments> typesToCheck() {
        return Stream.concat(CommonMappings.get(), Stream.of(MULTISET_CASE)).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(TypeMapping mapping) {
        final Descriptor descriptor =
                FlinkToProtoSchemaConverter.fromFlinkSchema(
                        (RowType) mapping.getFlinkType(), "Row", PACKAGE_NAME);

        assertThat(new ProtobufSchema(descriptor).toString().trim())
                .isEqualTo(mapping.getExpectedString().trim());
    }

    private static final TypeMapping MULTISET_CASE =
            new TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "message Row {\n"
                            + "  repeated MultisetEntry multiset = 1;\n"
                            + "\n"
                            + "  message MultisetEntry {\n"
                            + "    optional string key = 1;\n"
                            + "    int32 value = 2;\n"
                            + "  }\n"
                            + "}",
                    new RowType(
                            false,
                            Collections.singletonList(
                                    new RowField(
                                            "multiset",
                                            new MultisetType(
                                                    true,
                                                    new VarCharType(
                                                            true, VarCharType.MAX_LENGTH))))));
}
