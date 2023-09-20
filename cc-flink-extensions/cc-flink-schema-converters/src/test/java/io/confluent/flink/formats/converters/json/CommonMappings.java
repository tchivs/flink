/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_INDEX_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_INT8;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_TIMESTAMP;

/** Common data to use in schema mapping tests. */
public final class CommonMappings {

    public static final NumberSchema FLOAT64_SCHEMA =
            NumberSchema.builder()
                    .unprocessedProperties(
                            Collections.singletonMap(
                                    CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_FLOAT64))
                    .build();
    public static final NumberSchema INT64_SCHEMA =
            NumberSchema.builder()
                    .unprocessedProperties(
                            Collections.singletonMap(
                                    CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_INT64))
                    .build();
    public static final NumberSchema INT32_SCHEMA =
            NumberSchema.builder()
                    .unprocessedProperties(
                            Collections.singletonMap(
                                    CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_INT32))
                    .build();
    public static final NumberSchema FLOAT32_SCHEMA =
            NumberSchema.builder()
                    .unprocessedProperties(
                            Collections.singletonMap(
                                    CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_FLOAT32))
                    .build();
    public static final StringSchema BYTES_SCHEMA =
            StringSchema.builder()
                    .unprocessedProperties(
                            Collections.singletonMap(
                                    CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_BYTES))
                    .build();
    public static final ObjectSchema MAP_STRING_TINYINT_SCHEMA =
            ObjectSchema.builder()
                    .schemaOfAdditionalProperties(
                            NumberSchema.builder()
                                    .unprocessedProperties(
                                            Collections.singletonMap("connect.type", "int8"))
                                    .build())
                    .unprocessedProperties(Collections.singletonMap("connect.type", "map"))
                    .build();

    public static final NumberSchema TIMESTAMP_SCHEMA =
            NumberSchema.builder()
                    .title(CONNECT_TYPE_TIMESTAMP)
                    .unprocessedProperties(
                            Collections.singletonMap(
                                    CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_INT64))
                    .build();
    public static final ArraySchema MAP_TINYINT_SMALLINT =
            ArraySchema.builder()
                    .allItemSchema(
                            ObjectSchema.builder()
                                    .addPropertySchema(
                                            "key",
                                            NumberSchema.builder()
                                                    .unprocessedProperties(
                                                            Collections.singletonMap(
                                                                    "connect.type", "int8"))
                                                    .build())
                                    .addPropertySchema(
                                            "value",
                                            NumberSchema.builder()
                                                    .unprocessedProperties(
                                                            Collections.singletonMap(
                                                                    "connect.type", "int16"))
                                                    .build())
                                    .build())
                    .unprocessedProperties(Collections.singletonMap("connect.type", "map"))
                    .build();

    /** A mapping between corresponding Avro and Flink types. */
    public static class TypeMapping {

        private final Schema jsonSchema;
        private final LogicalType flinkType;

        public TypeMapping(Schema jsonSchema, LogicalType flinkType) {
            this.jsonSchema = jsonSchema;
            this.flinkType = flinkType;
        }

        public Schema getJsonSchema() {
            return jsonSchema;
        }

        public LogicalType getFlinkType() {
            return flinkType;
        }

        @Override
        public String toString() {
            return "jsonSchema=" + jsonSchema + ", flinkType=" + flinkType;
        }
    }

    public static Stream<TypeMapping> get() {
        return Stream.of(
                new TypeMapping(FLOAT64_SCHEMA, new DoubleType(false)),
                new TypeMapping(INT64_SCHEMA, new BigIntType(false)),
                new TypeMapping(INT32_SCHEMA, new IntType(false)),
                new TypeMapping(BooleanSchema.builder().build(), new BooleanType(false)),
                new TypeMapping(FLOAT32_SCHEMA, new FloatType(false)),
                new TypeMapping(BYTES_SCHEMA, new VarBinaryType(false, VarBinaryType.MAX_LENGTH)),
                new TypeMapping(
                        StringSchema.builder().build(),
                        new VarCharType(false, VarCharType.MAX_LENGTH)),
                new TypeMapping(nullable(FLOAT64_SCHEMA), new DoubleType()),
                new TypeMapping(nullable(INT64_SCHEMA), new BigIntType()),
                new TypeMapping(nullable(INT32_SCHEMA), new IntType()),
                new TypeMapping(nullable(BooleanSchema.builder().build()), new BooleanType()),
                new TypeMapping(nullable(FLOAT32_SCHEMA), new FloatType()),
                new TypeMapping(
                        nullable(BYTES_SCHEMA), new VarBinaryType(true, VarBinaryType.MAX_LENGTH)),
                new TypeMapping(
                        nullable(StringSchema.builder().build()),
                        new VarCharType(true, VarCharType.MAX_LENGTH)),
                new TypeMapping(
                        MAP_STRING_TINYINT_SCHEMA,
                        new MapType(
                                false,
                                new VarCharType(false, VarCharType.MAX_LENGTH),
                                new TinyIntType(false))),
                new TypeMapping(
                        nullable(MAP_STRING_TINYINT_SCHEMA),
                        new MapType(
                                true,
                                new VarCharType(false, VarCharType.MAX_LENGTH),
                                new TinyIntType(false))),
                new TypeMapping(TIMESTAMP_SCHEMA, new LocalZonedTimestampType(false, 3)),
                new TypeMapping(
                        MAP_TINYINT_SMALLINT,
                        new MapType(false, new TinyIntType(false), new SmallIntType(false))),
                new TypeMapping(
                        recordSchema(),
                        new RowType(
                                false,
                                Arrays.asList(
                                        new RowField("int8", new TinyIntType(true)),
                                        new RowField(
                                                "string",
                                                new VarCharType(false, VarCharType.MAX_LENGTH))))));
    }

    private static Schema recordSchema() {
        final Map<String, Object> numberProperties = new HashMap<>();
        numberProperties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT8);
        Schema numberSchema =
                nullableBuilder(
                                NumberSchema.builder()
                                        .unprocessedProperties(numberProperties)
                                        .build())
                        .unprocessedProperties(Collections.singletonMap(CONNECT_INDEX_PROP, 0))
                        .build();
        StringSchema stringSchema =
                StringSchema.builder()
                        .unprocessedProperties(Collections.singletonMap(CONNECT_INDEX_PROP, 1))
                        .build();
        return ObjectSchema.builder()
                .addPropertySchema("string", stringSchema)
                .addPropertySchema("int8", numberSchema)
                .addRequiredProperty("string")
                .title("io.confluent.row")
                .build();
    }

    public static Schema nullable(Schema schema) {
        return CombinedSchema.oneOf(Arrays.asList(NullSchema.builder().build(), schema)).build();
    }

    public static Schema.Builder<?> nullableBuilder(Schema schema) {
        return CombinedSchema.oneOf(Arrays.asList(NullSchema.builder().build(), schema));
    }

    private CommonMappings() {}
}
