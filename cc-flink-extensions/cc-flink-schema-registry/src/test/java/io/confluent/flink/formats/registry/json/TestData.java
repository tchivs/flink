/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.flink.formats.converters.json.CommonMappings.TypeMapping;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_MAP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_MULTISET;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_PROP;
import static java.util.Collections.singletonMap;

/** Data for testing JSON converters. */
class TestData {

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    /** Extends {@link TypeMapping} with data. */
    static class TypeMappingWithData {

        private final String description;
        private final TypeMapping typeMapping;
        private final Object flinkData;
        private final JsonNode jsonData;

        TypeMappingWithData(
                String description, TypeMapping typeMapping, Object flinkData, JsonNode jsonData) {
            this.description = description;
            this.typeMapping = typeMapping;
            this.flinkData = flinkData;
            this.jsonData = jsonData;
        }

        public Schema getJsonSchema() {
            return typeMapping.getJsonSchema();
        }

        public LogicalType getFlinkSchema() {
            return typeMapping.getFlinkType();
        }

        public Object getFlinkData() {
            return flinkData;
        }

        public JsonNode getJsonData() {
            return jsonData;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    static TypeMappingWithData getMultisetOfStrings() {
        NumberSchema countSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int32"))
                        .build();

        final Map<String, Object> props = new HashMap<>();
        props.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
        props.put(FLINK_TYPE_PROP, FLINK_TYPE_MULTISET);
        ObjectSchema multisetSchema =
                ObjectSchema.builder()
                        .schemaOfAdditionalProperties(countSchema)
                        .unprocessedProperties(props)
                        .build();

        final LogicalType flinkSchema =
                DataTypes.MULTISET(DataTypes.STRING().notNull()).notNull().getLogicalType();

        final Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put(StringData.fromString("ABC"), 16);
        inputMap.put(StringData.fromString("DEF"), 1);
        final GenericMapData flinkData = new GenericMapData(inputMap);

        final ObjectNode jsonData = OBJECT_MAPPER.createObjectNode();
        jsonData.put("ABC", 16);
        jsonData.put("DEF", 1);

        return new TypeMappingWithData(
                "MULTISET<STRING>",
                new TypeMapping(multisetSchema, flinkSchema),
                flinkData,
                jsonData);
    }

    static TypeMappingWithData getMultisetOfBigints() {
        NumberSchema valueSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int32"))
                        .build();
        NumberSchema countSchema =
                NumberSchema.builder()
                        .unprocessedProperties(singletonMap("connect.type", "int32"))
                        .build();
        ObjectSchema entrySchema =
                ObjectSchema.builder()
                        .addPropertySchema("key", valueSchema)
                        .addPropertySchema("value", countSchema)
                        .build();

        final Map<String, Object> props = new HashMap<>();
        props.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
        props.put(FLINK_TYPE_PROP, FLINK_TYPE_MULTISET);
        ArraySchema multisetSchema =
                ArraySchema.builder()
                        .allItemSchema(entrySchema)
                        .unprocessedProperties(props)
                        .build();

        final LogicalType flinkSchema =
                DataTypes.MULTISET(DataTypes.BIGINT().notNull()).notNull().getLogicalType();

        final Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put(420L, 16);
        inputMap.put(421L, 1);
        final GenericMapData flinkData = new GenericMapData(inputMap);

        ArrayNode jsonData = OBJECT_MAPPER.createArrayNode();
        jsonData.add(OBJECT_MAPPER.createObjectNode().put("key", 420L).put("value", 16));
        jsonData.add(OBJECT_MAPPER.createObjectNode().put("key", 421L).put("value", 1));
        return new TypeMappingWithData(
                "MULTISET<BIGINT>",
                new TypeMapping(multisetSchema, flinkSchema),
                flinkData,
                jsonData);
    }
}
