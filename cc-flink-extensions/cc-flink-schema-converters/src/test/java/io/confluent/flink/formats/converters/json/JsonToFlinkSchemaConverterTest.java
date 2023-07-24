/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.converters.json.CommonMappings.TypeMapping;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonToFlinkSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
public class JsonToFlinkSchemaConverterTest {

    public static Stream<Arguments> typesToCheck() {
        return Stream.concat(
                        CommonMappings.get(),
                        Stream.of(unionDifferentStruct(), combinedSchemaAllOfRef()))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(TypeMapping mapping) {
        assertThat(JsonToFlinkSchemaConverter.toFlinkSchema(mapping.getJsonSchema()))
                .isEqualTo(mapping.getFlinkType());
    }

    static TypeMapping unionDifferentStruct() {
        final Map<String, Object> numberSchemaProps = new HashMap<>();
        numberSchemaProps.put("connect.index", 0);
        numberSchemaProps.put("connect.type", "int8");
        NumberSchema numberSchema =
                NumberSchema.builder().unprocessedProperties(numberSchemaProps).build();
        StringSchema stringSchema =
                StringSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.index", 1))
                        .build();
        ObjectSchema firstSchema =
                ObjectSchema.builder()
                        .addPropertySchema("a", numberSchema)
                        .addPropertySchema("b", stringSchema)
                        .addRequiredProperty("a")
                        .title("field0")
                        .unprocessedProperties(Collections.singletonMap("connect.index", 0))
                        .build();
        ObjectSchema secondSchema =
                ObjectSchema.builder()
                        .addPropertySchema("c", numberSchema)
                        .addPropertySchema("d", stringSchema)
                        .addRequiredProperty("d")
                        .title("field1")
                        .unprocessedProperties(Collections.singletonMap("connect.index", 1))
                        .build();
        return new TypeMapping(
                CombinedSchema.oneOf(Arrays.asList(firstSchema, secondSchema)).build(),
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField(
                                        "connect_union_field_0",
                                        new RowType(
                                                true,
                                                Arrays.asList(
                                                        new RowField("a", new TinyIntType(false)),
                                                        new RowField(
                                                                "b",
                                                                new VarCharType(
                                                                        true,
                                                                        VarCharType.MAX_LENGTH))))),
                                new RowField(
                                        "connect_union_field_1",
                                        new RowType(
                                                true,
                                                Arrays.asList(
                                                        new RowField("c", new TinyIntType(true)),
                                                        new RowField(
                                                                "d",
                                                                new VarCharType(
                                                                        false,
                                                                        VarCharType
                                                                                .MAX_LENGTH))))))));
    }

    static TypeMapping combinedSchemaAllOfRef() {
        String schemaStr =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"allOf\": [\n"
                        + "    { \"$ref\": \"#/definitions/MessageBase\" },\n"
                        + "    { \"$ref\": \"#/definitions/MessageBase2\" }\n"
                        + "  ],\n"
                        + "\n"
                        + "  \"title\": \"IdentifyUserMessage\",\n"
                        + "  \"description\": \"An IdentifyUser message\",\n"
                        + "  \"type\": \"object\",\n"
                        + "\n"
                        + "  \"properties\": {\n"
                        + "    \"prevUserId\": {\n"
                        + "      \"type\": \"string\"\n"
                        + "    },\n"
                        + "\n"
                        + "    \"newUserId\": {\n"
                        + "      \"type\": \"string\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "\n"
                        + "  \"definitions\": {\n"
                        + "    \"MessageBase\": {\n"
                        + "      \"properties\": {\n"
                        + "        \"id\": {\n"
                        + "          \"type\": \"string\"\n"
                        + "        },\n"
                        + "\n"
                        + "        \"type\": {\n"
                        + "          \"type\": \"string\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    },\n"
                        + "    \"MessageBase2\": {\n"
                        + "      \"properties\": {\n"
                        + "        \"id2\": {\n"
                        + "          \"type\": \"string\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";
        JSONObject rawSchema = new JSONObject(new JSONTokener(new StringReader(schemaStr)));
        final VarCharType stringType = new VarCharType(VarCharType.MAX_LENGTH);
        return new TypeMapping(
                SchemaLoader.load(rawSchema),
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField("id", stringType),
                                new RowField("type", stringType),
                                new RowField("id2", stringType),
                                new RowField("newUserId", stringType),
                                new RowField("prevUserId", stringType))));
    }
}
