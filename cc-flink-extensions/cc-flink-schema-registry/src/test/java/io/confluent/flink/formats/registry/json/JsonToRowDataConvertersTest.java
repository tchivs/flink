/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.flink.formats.converters.json.JsonToFlinkSchemaConverter;
import io.confluent.flink.formats.registry.json.JsonToRowDataConverters.JsonToRowDataConverter;
import io.confluent.flink.formats.registry.json.TestData.TypeMappingWithData;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_INT64;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_TIMESTAMP;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_TIMESTAMP;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonToRowDataConverters}. */
class JsonToRowDataConvertersTest {

    @Test
    void testRowWithOptionalReference() throws Exception {
        String schemaString =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n"
                        + "  \"title\": \"Event\",\n"
                        + "  \"type\": \"object\",\n"
                        + "  \"properties\": {\n"
                        + "    \"vehicle\": {\n"
                        + "      \"$ref\": \"#/definitions/VehicleDto\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"definitions\": {\n"
                        + "    \"PriceDto\": {\n"
                        + "      \"type\": \"object\",\n"
                        + "      \"required\": [\n"
                        + "        \"amount\"\n"
                        + "      ],\n"
                        + "      \"properties\": {\n"
                        + "        \"amount\": {\n"
                        + "          \"type\": \"number\",\n"
                        + "          \"format\": \"decimal\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    },\n"
                        + "    \"VehicleDto\": {\n"
                        + "      \"type\": \"object\",\n"
                        + "      \"required\": [\n"
                        + "        \"offerPrice\",\n"
                        + "        \"firstRegistrationDate\"\n"
                        + "      ],\n"
                        + "      \"properties\": {\n"
                        + "        \"offerPrice\": {\n"
                        + "          \"connect.index\": 1,\n"
                        + "          \"$ref\": \"#/definitions/PriceDto\"\n"
                        + "        },\n"
                        + "        \"catalogPrice\": {\n"
                        + "          \"connect.index\": 2,\n"
                        + "          \"oneOf\": [\n"
                        + "            {\n"
                        + "              \"type\": \"null\"\n"
                        + "            },\n"
                        + "            {\n"
                        + "              \"$ref\": \"#/definitions/PriceDto\"\n"
                        + "            }\n"
                        + "          ]\n"
                        + "        },\n"
                        + "        \"firstRegistrationDate\": {\n"
                        + "          \"connect.index\": 3,\n"
                        + "          \"type\": \"string\",\n"
                        + "          \"format\": \"date-time\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}\n";
        JsonSchema jsonSchema = new JsonSchema(schemaString);
        ObjectSchema schema = (ObjectSchema) jsonSchema.rawSchema();

        String json =
                "{\"vehicle\":{\"offerPrice\":{\"amount\":20000},\"firstRegistrationDate\":\"2020-08-19T12:05:15.953Z\"}}";
        ObjectNode obj = (ObjectNode) Jackson.newObjectMapper().readTree(json);
        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);

        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);

        final RowData row = (RowData) converter.convert(obj);

        final GenericRowData expected = new GenericRowData(1);
        final GenericRowData vehicle = new GenericRowData(3);
        final GenericRowData offerPrice = new GenericRowData(1);
        offerPrice.setField(0, 20000D);
        vehicle.setField(0, offerPrice);
        vehicle.setField(2, StringData.fromString("2020-08-19T12:05:15.953Z"));
        expected.setField(0, vehicle);
        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testMapNonStringKeys() throws Exception {
        NumberSchema keySchema =
                NumberSchema.builder()
                        .requiresInteger(true)
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema valueSchema =
                NumberSchema.builder()
                        .requiresInteger(true)
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
                        .build();
        ObjectSchema mapSchema =
                ObjectSchema.builder()
                        .addPropertySchema("key", keySchema)
                        .addPropertySchema("value", valueSchema)
                        .build();
        ArraySchema schema =
                ArraySchema.builder()
                        .allItemSchema(mapSchema)
                        .unprocessedProperties(Collections.singletonMap("connect.type", "map"))
                        .build();

        ObjectNode map = JsonNodeFactory.instance.objectNode();
        map.set("key", IntNode.valueOf(12));
        map.set("value", ShortNode.valueOf((short) 16));
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        array.add(map);

        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);
        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);

        final MapData result = (MapData) converter.convert(array);
        final Map<Object, Object> resultMap = new HashMap<>();
        resultMap.put((byte) 12, (short) 16);
        assertThat(result).isEqualTo(new GenericMapData(resultMap));
    }

    @Test
    void testNullField() throws Exception {
        NumberSchema numberSchema =
                NumberSchema.builder()
                        .unprocessedProperties(mapOf("connect.index", 0, "connect.type", "int8"))
                        .build();
        NumberSchema longSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int64"))
                        .build();
        CombinedSchema oneof =
                CombinedSchema.oneOf(Arrays.asList(NullSchema.INSTANCE, longSchema))
                        .unprocessedProperties(Collections.singletonMap("connect.index", 1))
                        .build();
        ObjectSchema schema =
                ObjectSchema.builder()
                        .addPropertySchema("int8", numberSchema)
                        .addPropertySchema("long", oneof)
                        .title("Record")
                        .build();
        ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set("int8", ShortNode.valueOf((short) 12));
        obj.set("long", NullNode.getInstance());

        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);
        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);
        final RowData row = (RowData) converter.convert(obj);
        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, (byte) 12);
        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testArrayNullableWithProperties() throws Exception {
        String schemaString =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n"
                        + "  \"title\": \"Event\",\n"
                        + "  \"type\": \"object\",\n"
                        + "  \"properties\": {\n"
                        + "    \"rental_methods\": {\n"
                        + "      \"description\": \"Payment methods accepted at this station.\",\n"
                        + "      \"oneOf\": [\n"
                        + "        {\n"
                        + "          \"title\": \"Not included\",\n"
                        + "          \"type\": \"null\"\n"
                        + "        },\n"
                        + "        {\n"
                        + "          \"items\": {\n"
                        + "            \"enum\": [\n"
                        + "              \"KEY\",\n"
                        + "              \"CREDITCARD\",\n"
                        + "              \"PAYPASS\",\n"
                        + "              \"APPLEPAY\",\n"
                        + "              \"ANDROIDPAY\",\n"
                        + "              \"TRANSITCARD\",\n"
                        + "              \"ACCOUNTNUMBER\",\n"
                        + "              \"PHONE\"\n"
                        + "            ],\n"
                        + "            \"type\": \"string\"\n"
                        + "          },\n"
                        + "          \"type\": \"array\"\n"
                        + "        }\n"
                        + "      ]\n"
                        + "    }"
                        + "  }\n"
                        + "}\n";
        JsonSchema jsonSchema = new JsonSchema(schemaString);
        ObjectSchema schema = (ObjectSchema) jsonSchema.rawSchema();

        String json = "{\"rental_methods\": []}";
        ObjectNode obj = (ObjectNode) Jackson.newObjectMapper().readTree(json);
        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);

        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);

        final RowData row = (RowData) converter.convert(obj);

        final GenericRowData expected = new GenericRowData(1);
        expected.setField(0, new GenericArrayData(new Object[0]));
        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testNullableNestedRow() throws Exception {
        NumberSchema numberSchema =
                NumberSchema.builder()
                        .unprocessedProperties(mapOf("connect.index", 0, "connect.type", "int8"))
                        .build();
        ObjectSchema nestedSchema =
                ObjectSchema.builder().addPropertySchema("a", numberSchema).build();
        CombinedSchema oneof =
                CombinedSchema.oneOf(Arrays.asList(NullSchema.INSTANCE, nestedSchema))
                        .unprocessedProperties(Collections.singletonMap("connect.index", 1))
                        .build();
        ObjectSchema schema =
                ObjectSchema.builder().addPropertySchema("nested", oneof).title("Record").build();
        ObjectNode obj = JsonNodeFactory.instance.objectNode();
        ObjectNode nested = JsonNodeFactory.instance.objectNode();
        nested.set("a", ShortNode.valueOf((short) 42));
        obj.set("nested", nested);

        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);
        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);
        final RowData row = (RowData) converter.convert(obj);
        final GenericRowData expected = new GenericRowData(1);
        final GenericRowData nestedRow = new GenericRowData(1);
        nestedRow.setField(0, (byte) 42);
        expected.setField(0, nestedRow);

        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testOneOf() throws Exception {
        NumberSchema firstSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema secondSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
                        .build();
        CombinedSchema schema =
                CombinedSchema.oneOf(Arrays.asList(firstSchema, secondSchema)).build();

        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);

        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);
        final RowData result = (RowData) converter.convert(ShortNode.valueOf((short) 12));
        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, (byte) 12);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testAnyOf() throws Exception {
        NumberSchema firstSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema secondSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
                        .build();
        CombinedSchema schema =
                CombinedSchema.anyOf(Arrays.asList(firstSchema, secondSchema)).build();

        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);

        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);
        final RowData result = (RowData) converter.convert(ShortNode.valueOf((short) 12));
        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, (byte) 12);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testAllOf() throws Exception {
        NumberSchema firstSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema secondSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
                        .build();
        NumberSchema thirdSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
                        .build();

        ObjectSchema objectSchema1 =
                ObjectSchema.builder()
                        .addPropertySchema("a", firstSchema)
                        .addPropertySchema("b", secondSchema)
                        .title("Record1")
                        .build();

        ObjectSchema objectSchema2 =
                ObjectSchema.builder()
                        .addPropertySchema("c", secondSchema)
                        .addPropertySchema("d", thirdSchema)
                        .title("Record2")
                        .build();

        CombinedSchema schema =
                CombinedSchema.allOf(Arrays.asList(objectSchema1, objectSchema2)).build();
        ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set("a", ShortNode.valueOf((short) 42));
        obj.set("b", ShortNode.valueOf((short) 12));
        obj.set("c", NullNode.getInstance());
        obj.set("d", IntNode.valueOf(123));

        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);

        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);
        final RowData result = (RowData) converter.convert(obj);
        final GenericRowData expected = new GenericRowData(4);
        expected.setField(0, (byte) 42);
        expected.setField(1, (short) 12);
        expected.setField(3, 123);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testComplexAuditLogging() throws Exception {
        String schemaString =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n"
                        + "  \"title\": \"Event\",\n"
                        + "  \"type\": \"object\",\n"
                        + "  \"properties\": {\n"
                        + "     \"ip\": {\n"
                        + "        \"type\": \"string\",\n"
                        + "        \"description\": \"IPv4 or IPv6 address.\",\n"
                        + "        \"anyOf\": [\n"
                        + "            {\n"
                        + "                \"format\": \"ipv4\"\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"format\": \"ipv6\"\n"
                        + "            }\n"
                        + "        ]\n"
                        + "     }\n"
                        + "  }\n"
                        + "}\n";

        JsonSchema jsonSchema = new JsonSchema(schemaString);
        ObjectSchema schema = (ObjectSchema) jsonSchema.rawSchema();
        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);
        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(schema, flinkSchema);

        final ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set("ip", TextNode.valueOf("192.168.0.1"));

        final GenericRowData expected = new GenericRowData(1);
        final GenericRowData nested = new GenericRowData(2);
        nested.setField(0, StringData.fromString("192.168.0.1"));
        expected.setField(0, nested);

        assertThat(converter.convert(obj)).isEqualTo(expected);
    }

    @Test
    void testTimestampLtz() throws IOException {
        NumberSchema timestampSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64))
                        .title(CONNECT_TYPE_TIMESTAMP)
                        .build();

        final LogicalType flinkSchema = DataTypes.TIMESTAMP_LTZ(3).getLogicalType();

        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(timestampSchema, flinkSchema);
        final long milliseconds = 123456;
        final TimestampData timestampData = TimestampData.fromEpochMillis(milliseconds);
        final Object result = converter.convert(LongNode.valueOf(milliseconds));
        assertThat(result).isEqualTo(timestampData);
    }

    @Test
    void testTimestamp() throws Exception {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64);
        properties.put(FLINK_TYPE_PROP, FLINK_TYPE_TIMESTAMP);
        NumberSchema timestampSchema =
                NumberSchema.builder().unprocessedProperties(properties).build();

        final LogicalType flinkSchema = DataTypes.TIMESTAMP(3).getLogicalType();

        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(timestampSchema, flinkSchema);
        final long milliseconds = 123456;
        final TimestampData timestampData = TimestampData.fromEpochMillis(milliseconds);
        final Object result = converter.convert(LongNode.valueOf(milliseconds));
        assertThat(result).isEqualTo(timestampData);
    }

    static Stream<Arguments> multisetTests() {
        return Stream.of(TestData.getMultisetOfBigints(), TestData.getMultisetOfStrings())
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("multisetTests")
    void testMultiset(TypeMappingWithData mapping) throws Exception {
        final JsonToRowDataConverter converter =
                JsonToRowDataConverters.createConverter(
                        mapping.getJsonSchema(), mapping.getFlinkSchema());

        final Object result = converter.convert(mapping.getJsonData());

        assertThat(result).isEqualTo(mapping.getFlinkData());
    }

    public static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2) {
        final Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}
