/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.protobuf;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.flink.formats.converters.utils.SchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/** Common data to use in schema mapping tests. */
public final class CommonMappings {

    /** A mapping between corresponding Avro and Flink types. */
    public static class TypeMapping {

        private final Descriptor protoSchema;
        private final LogicalType flinkType;
        private final String schemaStr;

        public TypeMapping(String schemaStr, LogicalType flinkType) {
            this.schemaStr = schemaStr;
            this.protoSchema = new ProtobufSchema(schemaStr).toDescriptor();
            this.flinkType = flinkType;
        }

        public Descriptor getProtoSchema() {
            return protoSchema;
        }

        public String getExpectedString() {
            return schemaStr;
        }

        public LogicalType getFlinkType() {
            return flinkType;
        }

        @Override
        public String toString() {
            return "protoSchema=" + schemaStr + ", flinkType=" + flinkType;
        }
    }

    public static Stream<TypeMapping> get() {
        return Stream.of(
                NESTED_ROWS_CASE,
                NESTED_ROWS_SAME_NAME,
                ALL_SIMPLE_TYPES_CASE,
                COLLECTIONS_CASE,
                STRING_TYPES_CASE,
                NOT_NULL_MESSAGE_TYPES_CASE,
                NESTED_ROW_NOT_NULL_CASE,
                NULLABLE_ARRAYS_CASE,
                NULLABLE_COLLECTIONS_CASE);
    }

    private static final TypeMapping NESTED_ROWS_CASE =
            new TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "message Row {\n"
                            + "  optional meta_Row meta = 1;\n"
                            + "\n"
                            + "  message meta_Row {\n"
                            + "    optional tags_Row tags = 1;\n"
                            + "  \n"
                            + "    message tags_Row {\n"
                            + "      float a = 1;\n"
                            + "      float b = 2;\n"
                            + "    }\n"
                            + "  }\n"
                            + "}\n",
                    new RowType(
                            false,
                            Collections.singletonList(
                                    new RowField(
                                            "meta",
                                            new RowType(
                                                    Collections.singletonList(
                                                            new RowField(
                                                                    "tags",
                                                                    new RowType(
                                                                            asList(
                                                                                    new RowField(
                                                                                            "a",
                                                                                            new FloatType(
                                                                                                    false)),
                                                                                    new RowField(
                                                                                            "b",
                                                                                            new FloatType(
                                                                                                    false)))))))))));

    private static final TypeMapping NESTED_ROWS_SAME_NAME =
            new TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "message Row {\n"
                            + "  optional b_Row b = 1;\n"
                            + "\n"
                            + "  message b_Row {\n"
                            + "    optional b_Row b = 1;\n"
                            + "  \n"
                            + "    message b_Row {\n"
                            + "      optional float a = 1;\n"
                            + "    }\n"
                            + "  }\n"
                            + "}\n",
                    new RowType(
                            false,
                            Collections.singletonList(
                                    new RowField(
                                            "b",
                                            new RowType(
                                                    Collections.singletonList(
                                                            new RowField(
                                                                    "b",
                                                                    new RowType(
                                                                            Collections
                                                                                    .singletonList(
                                                                                            new RowField(
                                                                                                    "a",
                                                                                                    new FloatType()))))))))));

    private static final TypeMapping COLLECTIONS_CASE =
            new TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "message Row {\n"
                            + "  repeated int64 array = 1;\n"
                            + "  repeated MapEntry map = 2;\n"
                            + "\n"
                            + "  message MapEntry {\n"
                            + "    optional string key = 1;\n"
                            + "    optional int64 value = 2;\n"
                            + "  }\n"
                            + "}",
                    new RowType(
                            false,
                            Arrays.asList(
                                    new RowField(
                                            "array", new ArrayType(false, new BigIntType(false))),
                                    new RowField(
                                            "map",
                                            new MapType(
                                                    false,
                                                    new VarCharType(true, VarCharType.MAX_LENGTH),
                                                    new BigIntType(true))))));

    private static final TypeMapping ALL_SIMPLE_TYPES_CASE =
            new TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "import \"confluent/type/decimal.proto\";\n"
                            + "import \"google/protobuf/timestamp.proto\";\n"
                            + "import \"google/type/date.proto\";\n"
                            + "import \"google/type/timeofday.proto\";\n"
                            + "\n"
                            + "message Row {\n"
                            + "  bool booleanNotNull = 1;\n"
                            + "  bool boolean = 2;\n"
                            + "  int32 tinyIntNotNull = 3 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"connect.type\",\n"
                            + "        value: \"int8\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional int32 tinyInt = 4 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"connect.type\",\n"
                            + "        value: \"int8\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  int32 smallIntNotNull = 5 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"connect.type\",\n"
                            + "        value: \"int16\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional int32 smallInt = 6 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"connect.type\",\n"
                            + "        value: \"int16\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  int32 intNotNull = 7;\n"
                            + "  optional int32 int = 8;\n"
                            + "  int64 bigintNotNull = 9;\n"
                            + "  optional int64 bigint = 10;\n"
                            + "  double doubleNotNull = 11;\n"
                            + "  optional double double = 12;\n"
                            + "  float floatNotNull = 13;\n"
                            + "  optional float float = 14;\n"
                            + "  optional .google.type.Date date = 15;\n"
                            + "  optional .confluent.type.Decimal decimal = 16 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        value: \"5\",\n"
                            + "        key: \"precision\"\n"
                            + "      },\n"
                            + "      {\n"
                            + "        value: \"1\",\n"
                            + "        key: \"scale\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional .google.protobuf.Timestamp timestamp_ltz = 17;\n"
                            + "  optional .google.protobuf.Timestamp timestamp_ltz_3 = 18 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.precision\",\n"
                            + "        value: \"3\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional .google.protobuf.Timestamp timestamp = 19 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.type\",\n"
                            + "        value: \"timestamp\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional .google.protobuf.Timestamp timestamp_3 = 20 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.precision\",\n"
                            + "        value: \"3\"\n"
                            + "      },\n"
                            + "      {\n"
                            + "        key: \"flink.type\",\n"
                            + "        value: \"timestamp\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional .google.type.TimeOfDay time = 21 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.precision\",\n"
                            + "        value: \"3\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional .google.type.TimeOfDay time_2 = 22 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.precision\",\n"
                            + "        value: \"2\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "}",
                    new RowType(
                            false,
                            Arrays.asList(
                                    new RowField("booleanNotNull", new BooleanType(false)),
                                    new RowField("boolean", new BooleanType(false)),
                                    new RowField("tinyIntNotNull", new TinyIntType(false)),
                                    new RowField("tinyInt", new TinyIntType(true)),
                                    new RowField("smallIntNotNull", new SmallIntType(false)),
                                    new RowField("smallInt", new SmallIntType(true)),
                                    new RowField("intNotNull", new IntType(false)),
                                    new RowField("int", new IntType(true)),
                                    new RowField("bigintNotNull", new BigIntType(false)),
                                    new RowField("bigint", new BigIntType(true)),
                                    new RowField("doubleNotNull", new DoubleType(false)),
                                    new RowField("double", new DoubleType(true)),
                                    new RowField("floatNotNull", new FloatType(false)),
                                    new RowField("float", new FloatType(true)),
                                    new RowField("date", new DateType(true)),
                                    new RowField("decimal", new DecimalType(true, 5, 1)),
                                    new RowField(
                                            "timestamp_ltz", new LocalZonedTimestampType(true, 9)),
                                    new RowField(
                                            "timestamp_ltz_3",
                                            new LocalZonedTimestampType(true, 3)),
                                    new RowField("timestamp", new TimestampType(true, 9)),
                                    new RowField("timestamp_3", new TimestampType(true, 3)),
                                    new RowField("time", new TimeType(true, 3)),
                                    new RowField("time_2", new TimeType(true, 2)))));

    private static final TypeMapping STRING_TYPES_CASE =
            new TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "message Row {\n"
                            + "  optional string string = 1;\n"
                            + "  optional string charMax = 2 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.maxLength\",\n"
                            + "        value: \"2147483647\"\n"
                            + "      },\n"
                            + "      {\n"
                            + "        key: \"flink.minLength\",\n"
                            + "        value: \"2147483647\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional bytes bytes = 3;\n"
                            + "  optional bytes binaryMax = 4 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.maxLength\",\n"
                            + "        value: \"2147483647\"\n"
                            + "      },\n"
                            + "      {\n"
                            + "        key: \"flink.minLength\",\n"
                            + "        value: \"2147483647\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional string varchar = 5 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.maxLength\",\n"
                            + "        value: \"123\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional bytes varbinary = 6 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.maxLength\",\n"
                            + "        value: \"123\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional string char = 7 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.maxLength\",\n"
                            + "        value: \"123\"\n"
                            + "      },\n"
                            + "      {\n"
                            + "        key: \"flink.minLength\",\n"
                            + "        value: \"123\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "  optional bytes binary = 8 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.maxLength\",\n"
                            + "        value: \"123\"\n"
                            + "      },\n"
                            + "      {\n"
                            + "        key: \"flink.minLength\",\n"
                            + "        value: \"123\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "}",
                    new RowType(
                            false,
                            Arrays.asList(
                                    new RowField(
                                            "string",
                                            new VarCharType(true, VarCharType.MAX_LENGTH)),
                                    new RowField(
                                            "charMax", new CharType(true, CharType.MAX_LENGTH)),
                                    new RowField(
                                            "bytes",
                                            new VarBinaryType(true, VarBinaryType.MAX_LENGTH)),
                                    new RowField(
                                            "binaryMax",
                                            new BinaryType(true, BinaryType.MAX_LENGTH)),
                                    new RowField("varchar", new VarCharType(true, 123)),
                                    new RowField("varbinary", new VarBinaryType(true, 123)),
                                    new RowField("char", new CharType(true, 123)),
                                    new RowField("binary", new BinaryType(true, 123)))));

    private static final TypeMapping NOT_NULL_MESSAGE_TYPES_CASE =
            new TypeMapping(
                    SchemaUtils.readSchemaFromResource("schema/proto/not_null_message_types.proto"),
                    new RowType(
                            false,
                            Arrays.asList(
                                    new RowField("date", new DateType(false)),
                                    new RowField("decimal", new DecimalType(false, 5, 1)),
                                    new RowField(
                                            "timestamp_ltz", new LocalZonedTimestampType(false, 9)),
                                    new RowField("timestamp", new TimestampType(false, 9)),
                                    new RowField("time", new TimeType(false, 3)))));

    private static final TypeMapping NESTED_ROW_NOT_NULL_CASE =
            new TypeMapping(
                    "syntax = \"proto3\";\n"
                            + "package io.confluent.protobuf.generated;\n"
                            + "\n"
                            + "message Row {\n"
                            + "  meta_Row meta = 1 [(confluent.field_meta) = {\n"
                            + "    params: [\n"
                            + "      {\n"
                            + "        key: \"flink.notNull\",\n"
                            + "        value: \"true\"\n"
                            + "      }\n"
                            + "    ]\n"
                            + "  }];\n"
                            + "\n"
                            + "  message meta_Row {\n"
                            + "    float a = 1;\n"
                            + "    float b = 2;\n"
                            + "  }\n"
                            + "}\n",
                    new RowType(
                            false,
                            Collections.singletonList(
                                    new RowField(
                                            "meta",
                                            new RowType(
                                                    false,
                                                    asList(
                                                            new RowField("a", new FloatType(false)),
                                                            new RowField(
                                                                    "b",
                                                                    new FloatType(false))))))));

    /** Mix of arrays and nullability. */
    public static final TypeMapping NULLABLE_ARRAYS_CASE =
            new TypeMapping(
                    SchemaUtils.readSchemaFromResource("schema/proto/nullable_arrays.proto"),
                    new RowType(
                            false,
                            Arrays.asList(
                                    new RowField(
                                            "arrayNullable",
                                            new ArrayType(true, new BigIntType(false))),
                                    new RowField(
                                            "elementNullable",
                                            new ArrayType(false, new BigIntType(true))),
                                    new RowField(
                                            "arrayAndElementNullable",
                                            new ArrayType(true, new BigIntType(true))))));

    /** Mix of collections (arrays + maps) and nullability. */
    public static final TypeMapping NULLABLE_COLLECTIONS_CASE =
            new TypeMapping(
                    SchemaUtils.readSchemaFromResource("schema/proto/nullable_collections.proto"),
                    new RowType(
                            false,
                            Arrays.asList(
                                    new RowField(
                                            "nullableMap",
                                            new MapType(
                                                    true,
                                                    new BigIntType(false),
                                                    new BigIntType(false))),
                                    new RowField(
                                            "arrayOfMaps",
                                            new ArrayType(
                                                    false,
                                                    new MapType(
                                                            false,
                                                            new BigIntType(false),
                                                            new BigIntType(false)))),
                                    new RowField(
                                            "nullableArrayOfNullableMaps",
                                            new ArrayType(
                                                    true,
                                                    new MapType(
                                                            true,
                                                            new BigIntType(false),
                                                            new BigIntType(false)))),
                                    new RowField(
                                            "mapOfNullableArrays",
                                            new MapType(
                                                    false,
                                                    new BigIntType(false),
                                                    new ArrayType(true, new BigIntType(false)))))));

    private CommonMappings() {}
}
