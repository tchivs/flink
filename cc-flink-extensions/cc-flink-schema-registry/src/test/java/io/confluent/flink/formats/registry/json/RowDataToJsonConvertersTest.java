/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import io.confluent.flink.formats.registry.json.RowDataToJsonConverters.RowDataToJsonConverter;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_INT64;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_TIMESTAMP;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonToRowDataConverters}. */
class RowDataToJsonConvertersTest {

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    @Test
    void testMapNonStringKeys() throws Exception {
        NumberSchema keySchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema valueSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int16"))
                        .build();
        ObjectSchema mapSchema =
                ObjectSchema.builder()
                        .addPropertySchema("key", keySchema)
                        .addPropertySchema("value", valueSchema)
                        .build();
        ArraySchema schema =
                ArraySchema.builder()
                        .allItemSchema(mapSchema)
                        .unprocessedProperties(singletonMap("connect.type", "map"))
                        .build();

        final LogicalType flinkSchema =
                DataTypes.MAP(DataTypes.TINYINT().notNull(), DataTypes.SMALLINT().notNull())
                        .notNull()
                        .getLogicalType();

        final RowDataToJsonConverter converter =
                RowDataToJsonConverters.createConverter(flinkSchema, schema);

        final Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put((byte) 12, (short) 16);
        final GenericMapData mapData = new GenericMapData(inputMap);
        final JsonNode result = converter.convert(OBJECT_MAPPER, null, mapData);

        ObjectNode map = OBJECT_MAPPER.createObjectNode();
        map.set("key", ShortNode.valueOf((short) 12));
        map.set("value", ShortNode.valueOf((short) 16));
        ArrayNode expected = OBJECT_MAPPER.createArrayNode();
        expected.add(map);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testNullField() throws Exception {
        NumberSchema numberSchema =
                NumberSchema.builder()
                        .unprocessedProperties(mapOf("connect.index", 0, "connect.type", "int8"))
                        .build();
        NumberSchema longSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int64"))
                        .build();
        CombinedSchema oneof =
                CombinedSchema.oneOf(Arrays.asList(NullSchema.INSTANCE, longSchema))
                        .unprocessedProperties(singletonMap("connect.index", 1))
                        .build();
        ObjectSchema schema =
                ObjectSchema.builder()
                        .addPropertySchema("int8", numberSchema)
                        .addPropertySchema("long", oneof)
                        .title("Record")
                        .build();
        final LogicalType flinkSchema =
                DataTypes.ROW(
                                DataTypes.FIELD("int8", DataTypes.TINYINT().notNull()),
                                DataTypes.FIELD("long", DataTypes.BIGINT().nullable()))
                        .notNull()
                        .getLogicalType();
        final RowDataToJsonConverter converter =
                RowDataToJsonConverters.createConverter(flinkSchema, schema);
        final GenericRowData input = new GenericRowData(2);
        input.setField(0, (byte) 12);
        final JsonNode result = converter.convert(OBJECT_MAPPER, null, input);

        ObjectNode expected = OBJECT_MAPPER.createObjectNode();
        expected.set("int8", ShortNode.valueOf((short) 12));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testNullableNestedRow() throws Exception {
        NumberSchema numberSchema =
                NumberSchema.builder()
                        .unprocessedProperties(mapOf("connect.index", 0, "connect.type", "int8"))
                        .build();
        ObjectSchema nestedSchema =
                ObjectSchema.builder()
                        .addPropertySchema("a", numberSchema)
                        .addRequiredProperty("a")
                        .build();
        CombinedSchema oneof =
                CombinedSchema.oneOf(Arrays.asList(NullSchema.INSTANCE, nestedSchema))
                        .unprocessedProperties(singletonMap("connect.index", 1))
                        .build();
        ObjectSchema schema =
                ObjectSchema.builder().addPropertySchema("nested", oneof).title("Record").build();

        final LogicalType flinkSchema =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "nested",
                                        DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "a", DataTypes.TINYINT().notNull()))
                                                .nullable()))
                        .notNull()
                        .getLogicalType();

        final RowDataToJsonConverter converter =
                RowDataToJsonConverters.createConverter(flinkSchema, schema);
        final GenericRowData input = new GenericRowData(1);
        final GenericRowData nestedRow = new GenericRowData(1);
        nestedRow.setField(0, (byte) 42);
        input.setField(0, nestedRow);
        final JsonNode row = converter.convert(OBJECT_MAPPER, null, input);

        ObjectNode expected = OBJECT_MAPPER.createObjectNode();
        ObjectNode nested = OBJECT_MAPPER.createObjectNode();
        nested.set("a", ShortNode.valueOf((short) 42));
        expected.set("nested", nested);
        assertThat(row).isEqualTo(expected);
    }

    @Test
    void testOneOf() throws Exception {
        NumberSchema firstSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema secondSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int16"))
                        .build();
        CombinedSchema schema =
                CombinedSchema.oneOf(Arrays.asList(firstSchema, secondSchema)).build();

        final LogicalType flinkSchema =
                DataTypes.ROW(
                                DataTypes.FIELD("int8", DataTypes.TINYINT().nullable()),
                                DataTypes.FIELD("int16", DataTypes.SMALLINT().nullable()))
                        .notNull()
                        .getLogicalType();

        final RowDataToJsonConverter converter =
                RowDataToJsonConverters.createConverter(flinkSchema, schema);
        final GenericRowData input = new GenericRowData(2);
        input.setField(0, (byte) 12);
        final JsonNode result = converter.convert(OBJECT_MAPPER, null, input);
        assertThat(result).isEqualTo(ShortNode.valueOf((short) 12));
    }

    @Test
    void testAnyOf() throws Exception {
        NumberSchema firstSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema secondSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int16"))
                        .build();
        CombinedSchema schema =
                CombinedSchema.anyOf(Arrays.asList(firstSchema, secondSchema)).build();

        final LogicalType flinkSchema =
                DataTypes.ROW(
                                DataTypes.FIELD("int8", DataTypes.TINYINT().nullable()),
                                DataTypes.FIELD("int16", DataTypes.SMALLINT().nullable()))
                        .notNull()
                        .getLogicalType();

        final RowDataToJsonConverter converter =
                RowDataToJsonConverters.createConverter(flinkSchema, schema);
        final GenericRowData input = new GenericRowData(2);
        input.setField(0, (byte) 12);
        final JsonNode result = converter.convert(OBJECT_MAPPER, null, input);
        assertThat(result).isEqualTo(ShortNode.valueOf((short) 12));
    }

    @Test
    void testAllOf() throws Exception {
        NumberSchema firstSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema secondSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int16"))
                        .build();
        NumberSchema thirdSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int32"))
                        .build();

        ObjectSchema objectSchema1 =
                ObjectSchema.builder()
                        .addPropertySchema("a", firstSchema)
                        .addPropertySchema("b", secondSchema)
                        .title("Record1")
                        .build();

        ObjectSchema objectSchema2 =
                ObjectSchema.builder()
                        .addPropertySchema(
                                "c",
                                CombinedSchema.allOf(
                                                Arrays.asList(NullSchema.INSTANCE, secondSchema))
                                        .build())
                        .addPropertySchema("d", thirdSchema)
                        .addRequiredProperty("c")
                        .title("Record2")
                        .build();

        CombinedSchema schema =
                CombinedSchema.allOf(Arrays.asList(objectSchema1, objectSchema2)).build();

        final LogicalType flinkSchema =
                DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.TINYINT().nullable()),
                                DataTypes.FIELD("b", DataTypes.SMALLINT().nullable()),
                                DataTypes.FIELD("c", DataTypes.SMALLINT().nullable()),
                                DataTypes.FIELD("d", DataTypes.INT().nullable()))
                        .notNull()
                        .getLogicalType();

        final RowDataToJsonConverter converter =
                RowDataToJsonConverters.createConverter(flinkSchema, schema);
        final GenericRowData input = new GenericRowData(4);
        input.setField(0, (byte) 42);
        input.setField(1, (short) 12);
        input.setField(3, 123);
        final JsonNode result = converter.convert(OBJECT_MAPPER, null, input);

        ObjectNode expected = JsonNodeFactory.instance.objectNode();
        expected.set("a", ShortNode.valueOf((short) 42));
        expected.set("b", ShortNode.valueOf((short) 12));
        expected.set("c", NullNode.getInstance());
        expected.set("d", IntNode.valueOf(123));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testTimestampLtz() {
        NumberSchema timestampSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64))
                        .title(CONNECT_TYPE_TIMESTAMP)
                        .build();

        final LogicalType flinkSchema = DataTypes.TIMESTAMP_LTZ(3).getLogicalType();

        final RowDataToJsonConverter converter =
                RowDataToJsonConverters.createConverter(flinkSchema, timestampSchema);
        final long milliseconds = 123456;
        final TimestampData timestampData = TimestampData.fromEpochMillis(milliseconds);
        final JsonNode result = converter.convert(OBJECT_MAPPER, null, timestampData);
        assertThat(result).isEqualTo(LongNode.valueOf(milliseconds));
    }

    private static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2) {
        final Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}
