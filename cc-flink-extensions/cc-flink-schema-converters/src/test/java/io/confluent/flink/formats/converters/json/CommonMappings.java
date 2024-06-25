/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
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
import org.everit.json.schema.Schema.Builder;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_INDEX_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_BYTES;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_INT8;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_MAP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_TIMESTAMP;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_PRECISION;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_PROPERTY_CURRENT_VERSION;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_PROPERTY_VERSION;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_MULTISET;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_PROP;

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
        return Stream.concat(
                getPrimitiveTypes()
                        .flatMap(t -> Stream.of(t, toNullable(t)))
                        .flatMap(t -> Stream.of(t, toArrayType(t))),
                getCollectionTypes());
    }

    private static Stream<TypeMapping> getPrimitiveTypes() {
        return Stream.of(
                new TypeMapping(FLOAT64_SCHEMA, new DoubleType(false)),
                new TypeMapping(INT64_SCHEMA, new BigIntType(false)),
                new TypeMapping(INT32_SCHEMA, new IntType(false)),
                new TypeMapping(BooleanSchema.builder().build(), new BooleanType(false)),
                new TypeMapping(FLOAT32_SCHEMA, new FloatType(false)),
                new TypeMapping(BYTES_SCHEMA, new VarBinaryType(false, VarBinaryType.MAX_LENGTH)),
                new TypeMapping(createStringSchema(-1, 123), new VarCharType(false, 123)),
                new TypeMapping(createBinarySchema(-1, 123), new VarBinaryType(false, 123)),
                new TypeMapping(createStringSchema(123, 123), new CharType(false, 123)),
                new TypeMapping(createBinarySchema(123, 123), new BinaryType(false, 123)),
                new TypeMapping(
                        createBinarySchema(BinaryType.MAX_LENGTH, BinaryType.MAX_LENGTH),
                        new BinaryType(false, BinaryType.MAX_LENGTH)),
                new TypeMapping(createTimeSchema(3), new TimeType(false, 3)),
                new TypeMapping(createTimeSchema(2), new TimeType(false, 2)),
                new TypeMapping(createTimestampLtzSchema(3), new LocalZonedTimestampType(false, 3)),
                new TypeMapping(createTimestampLtzSchema(2), new LocalZonedTimestampType(false, 2)),
                new TypeMapping(createTimestampSchema(3), new TimestampType(false, 3)),
                new TypeMapping(createTimestampSchema(2), new TimestampType(false, 2)),
                new TypeMapping(
                        recordSchema(),
                        new RowType(
                                false,
                                Arrays.asList(
                                        new RowField("int8", new TinyIntType(true)),
                                        new RowField(
                                                "string",
                                                new VarCharType(false, VarCharType.MAX_LENGTH),
                                                "string field")))),
                new TypeMapping(
                        decimalSchema(),
                        new RowType(
                                false,
                                Collections.singletonList(
                                        new RowField("decimal", new DecimalType(10, 2))))));
    }

    private static Stream<TypeMapping> getCollectionTypes() {
        return Stream.of(
                new TypeMapping(
                        createArrayMapLikeSchema(INT64_SCHEMA, INT32_SCHEMA, true),
                        new MultisetType(false, new BigIntType(false))),
                new TypeMapping(
                        createObjectMapLikeSchema(INT32_SCHEMA, true),
                        new MultisetType(false, new VarCharType(false, VarCharType.MAX_LENGTH))),
                new TypeMapping(
                        createArrayMapLikeSchema(INT32_SCHEMA, INT64_SCHEMA, false),
                        new MapType(false, new IntType(false), new BigIntType(false))),
                new TypeMapping(
                        createObjectMapLikeSchema(INT64_SCHEMA, false),
                        new MapType(
                                false,
                                new VarCharType(false, VarCharType.MAX_LENGTH),
                                new BigIntType(false))));
    }

    private static Schema createObjectMapLikeSchema(Schema valueSchema, boolean isMultiset) {
        final ObjectSchema.Builder objectSchema = ObjectSchema.builder();

        final Map<String, Object> properties = new HashMap<>();

        properties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
        if (isMultiset) {
            properties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
            properties.put(FLINK_TYPE_PROP, FLINK_TYPE_MULTISET);
        }

        objectSchema.schemaOfAdditionalProperties(valueSchema);
        objectSchema.unprocessedProperties(properties);
        return objectSchema.build();
    }

    private static Schema createArrayMapLikeSchema(
            Schema keySchema, Schema valueSchema, boolean isMultiset) {
        final Map<String, Object> properties = new HashMap<>();

        properties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
        if (isMultiset) {
            properties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
            properties.put(FLINK_TYPE_PROP, FLINK_TYPE_MULTISET);
        }

        final ArraySchema.Builder builder =
                ArraySchema.builder()
                        .allItemSchema(
                                ObjectSchema.builder()
                                        .addPropertySchema("key", keySchema)
                                        .addPropertySchema("value", valueSchema)
                                        .build());

        builder.unprocessedProperties(properties);
        return builder.build();
    }

    private static Schema createStringSchema(int minLength, int maxLength) {
        final StringSchema.Builder builder = StringSchema.builder();
        if (minLength > 0) {
            builder.minLength(minLength);
        }
        builder.maxLength(maxLength);
        return builder.build();
    }

    private static Schema createBinarySchema(int minLength, int maxLength) {
        final StringSchema.Builder builder = StringSchema.builder();
        final Map<String, Object> props = new HashMap<>();
        if (minLength > 0) {
            props.put(CommonConstants.FLINK_MIN_LENGTH, minLength);
        }
        props.put(CommonConstants.FLINK_MAX_LENGTH, maxLength);
        props.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
        props.put(CONNECT_TYPE_PROP, CONNECT_TYPE_BYTES);
        return builder.unprocessedProperties(props).build();
    }

    private static Schema createTimeSchema(int precision) {
        Builder<NumberSchema> builder =
                NumberSchema.builder().title(CommonConstants.CONNECT_TYPE_TIME);
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_INT32);
        if (precision != 3) {
            properties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
            properties.put(FLINK_PRECISION, precision);
        }
        return builder.unprocessedProperties(properties).build();
    }

    private static Schema createTimestampSchema(int precision) {
        Builder<NumberSchema> builder = NumberSchema.builder();
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_INT64);
        properties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
        properties.put(FLINK_TYPE_PROP, CommonConstants.FLINK_TYPE_TIMESTAMP);
        if (precision != 3) {
            properties.put(FLINK_PRECISION, precision);
        }
        return builder.unprocessedProperties(properties).build();
    }

    private static NumberSchema createTimestampLtzSchema(int precision) {
        Builder<NumberSchema> builder = NumberSchema.builder().title(CONNECT_TYPE_TIMESTAMP);
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CONNECT_TYPE_PROP, CommonConstants.CONNECT_TYPE_INT64);
        if (precision != 3) {
            properties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
            properties.put(FLINK_PRECISION, precision);
        }
        return builder.unprocessedProperties(properties).build();
    }

    private static TypeMapping toNullable(TypeMapping mapping) {
        return new TypeMapping(
                nullable(mapping.getJsonSchema()), mapping.getFlinkType().copy(true));
    }

    private static TypeMapping toArrayType(TypeMapping mapping) {
        return new TypeMapping(
                ArraySchema.builder().allItemSchema(mapping.getJsonSchema()).build(),
                new ArrayType(false, mapping.getFlinkType()));
    }

    private static Schema decimalSchema() {
        // SQL-1593 case
        String schemaStr =
                "{\n"
                        + "  \"properties\": {\n"
                        + "    \"decimal\": {\n"
                        + "      \"connect.index\": 0,\n"
                        + "      \"oneOf\": [\n"
                        + "        {\n"
                        + "          \"type\": \"null\"\n"
                        + "        },\n"
                        + "        {\n"
                        + "          \"connect.parameters\": {\n"
                        + "            \"connect.decimal.precision\": \"10\",\n"
                        + "            \"scale\": \"2\"\n"
                        + "          },\n"
                        + "          \"connect.type\": \"bytes\",\n"
                        + "          \"title\": \"org.apache.kafka.connect.data.Decimal\",\n"
                        + "          \"type\": \"number\"\n"
                        + "        }\n"
                        + "      ]\n"
                        + "    }"
                        + "  },\n"
                        + "  \"title\": \"io.confluent.row\",\n"
                        + "  \"type\": \"object\"\n"
                        + "}";
        JSONObject rawSchema = new JSONObject(new JSONTokener(new StringReader(schemaStr)));
        return SchemaLoader.load(rawSchema);
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
                        .description("string field")
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
