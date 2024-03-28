/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.formats.converters.avro.util;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import io.confluent.flink.formats.converters.avro.CommonMappings;
import io.confluent.flink.formats.converters.avro.CommonMappings.TypeMapping;
import io.confluent.flink.formats.converters.avro.CommonMappings.TypeMappingWithData;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.confluent.flink.formats.converters.avro.AvroToFlinkSchemaConverter.unionMemberFieldName;

/** Arguments provider for testing union types. */
public class UnionUtil {

    /**
     * Creates a stream of type mappings of unions of 2 different types, and NULL added to the union
     * at positions 0, 1, and 2: (A, B) -> U(A, B), U(NULL, A,B), U(A, NULL, B), U(A, B, NULL).
     */
    public static class SchemaProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            Stream<TypeMapping> unionsWithoutNull =
                    createUnionTypeMappings(CommonMappings.getNotNull());
            Stream<TypeMapping> unionsWithNull =
                    IntStream.range(0, 3)
                            .mapToObj(
                                    i ->
                                            createUnionTypeMappings(CommonMappings.getNotNull())
                                                    .map(
                                                            typeMapping ->
                                                                    addNullToUnion(typeMapping, i)))
                            .flatMap(Function.identity());

            return Stream.concat(unionsWithoutNull, unionsWithNull).map(Arguments::of);
        }
    }

    /** Creates unions of all possible pairs of 2 different types. (A, B) -> U(A, B), U(B, A) */
    public static Stream<TypeMapping> createUnionTypeMappings(
            List<TypeMapping> singleTypeMappings) {
        return singleTypeMappings.stream()
                .flatMap(
                        first ->
                                singleTypeMappings.stream()
                                        .filter( // filter out two types with the same full name,
                                                // avro does not support a union of such types
                                                second ->
                                                        !first.getAvroSchema()
                                                                .getFullName()
                                                                .equals(
                                                                        second.getAvroSchema()
                                                                                .getFullName()))
                                        .filter( // filter out the same types
                                                second ->
                                                        !first.getFlinkType()
                                                                .equals(second.getFlinkType()))
                                        .map(second -> createTypeMapping(first, second)));
    }

    private static TypeMapping createTypeMapping(TypeMapping first, TypeMapping second) {
        return new TypeMapping(
                Schema.createUnion(first.getAvroSchema(), second.getAvroSchema()),
                RowType.of(
                        false, // row is not nullable
                        new LogicalType[] {
                            // fields are nullable
                            first.getFlinkType().copy(true), second.getFlinkType().copy(true)
                        },
                        new String[] {
                            unionMemberFieldName(first.getAvroSchema()),
                            unionMemberFieldName(second.getAvroSchema())
                        }));
    }

    /** Adds a NULL type to a union at a specified position. */
    private static TypeMapping addNullToUnion(TypeMapping typeMapping, int position) {
        List<Schema> unionTypes = new ArrayList<>(typeMapping.getAvroSchema().getTypes());
        unionTypes.add(position, Schema.create(Schema.Type.NULL));

        return new TypeMapping(
                Schema.createUnion(unionTypes),
                // Row and fields are nullable
                typeMapping.getFlinkType().copy(true));
    }

    /**
     * Adds data to a given {@link TypeMapping}. Expects the mapping is a union of two types U[A,
     * B]. As a result, produces a mapping of a record with two fields: ["union_type_1": U[A, B],
     * "union_type_2": U[A, B]] and data where the first field is populated with data of type A and
     * the second field of type B.
     */
    public static TypeMappingWithData withData(TypeMapping mapping) {
        // UNION_SCHEMA
        Schema unionSchema = mapping.getAvroSchema();

        // RECORD_SCHEMA[UNION_SCHEMA, UNION_SCHEMA]
        final String field1Name = "union_type_1";
        final String field2Name = "union_type_2";
        Schema avroSchema =
                SchemaBuilder.record("topLevelRow")
                        .namespace("io.confluent.test")
                        .fields()
                        .name(field1Name)
                        .type(unionSchema)
                        .noDefault()
                        .name(field2Name)
                        .type(unionSchema)
                        .noDefault()
                        .endRecord();

        // UNION_TYPE_1, UNION_TYPE_2, ... FROM UNION_SCHEMA
        List<Schema> types = unionSchema.getTypes();

        // RECORD[FIELD[UNION_TYPE_1],FIELD[UNION_TYPE_2]]
        GenericRecord avroRecordWithUnionTypeFields =
                new GenericRecordBuilder(avroSchema)
                        .set(field1Name, generateAvroFieldData(types.get(0)))
                        .set(field2Name, generateAvroFieldData(types.get(1)))
                        .build();

        // FLINK_SCHEMA
        List<LogicalType> flinkLogicalTypes = ((RowType) mapping.getFlinkType()).getChildren();

        GenericRowData expectedRowData = new GenericRowData(2);
        GenericRowData field1 = new GenericRowData(2);
        GenericRowData field2 = new GenericRowData(2);
        field1.setField(
                0,
                generateFlinkFieldData(
                        flinkLogicalTypes.get(0), types.get(0).getType() == Type.ENUM));
        field2.setField(
                1,
                generateFlinkFieldData(
                        flinkLogicalTypes.get(1), types.get(1).getType() == Type.ENUM));
        expectedRowData.setField(0, field1);
        expectedRowData.setField(1, field2);

        return new TypeMappingWithData(
                new TypeMapping(
                        avroSchema,
                        new RowType(
                                false,
                                Arrays.asList(
                                        new RowField(field1Name, mapping.getFlinkType()),
                                        new RowField(field2Name, mapping.getFlinkType())))),
                avroRecordWithUnionTypeFields,
                expectedRowData);
    }

    private static Object generateFlinkFieldData(LogicalType logicalType, boolean isEnum) {

        if (isEnum && !logicalType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new IllegalArgumentException("Nested enum types are not supported");
        }

        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return true;
            case TINYINT:
                return (byte) 1;
            case SMALLINT:
                return (short) 1;
            case INTEGER:
                return 1;
            case BIGINT:
                return 1L;
            case FLOAT:
                return 1.0f;
            case DOUBLE:
                return 1.0;
            case CHAR:
            case VARCHAR:
                if (isEnum) {
                    return StringData.fromString("red");
                } else {
                    return StringData.fromString("");
                }
            case DECIMAL:
                return DecimalData.fromBigDecimal(new BigDecimal("100.001"), 6, 3);
            case BINARY:
            case VARBINARY:
                return new byte[] {0, 1};
            case ROW:
                GenericRowData row = new GenericRowData(2);
                GenericRowData field1 = new GenericRowData(2);
                GenericRowData field2 = new GenericRowData(2);
                field1.setField(0, generateFlinkFieldData(logicalType.getChildren().get(0), false));
                field2.setField(1, generateFlinkFieldData(logicalType.getChildren().get(1), false));
                return row;
            case ARRAY:
                final Object[] array =
                        new Object[] {
                            generateFlinkFieldData(logicalType.getChildren().get(0), false)
                        };
                return new GenericArrayData(array);
            case MAP:
                final Map<Object, Object> map = new HashMap<>();
                map.put(
                        generateFlinkFieldData(logicalType.getChildren().get(0), false),
                        generateFlinkFieldData(logicalType.getChildren().get(1), false));
                return new GenericMapData(map);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromEpochMillis(10);
            default:
                throw new UnsupportedOperationException("Unsupported logical type: " + logicalType);
        }
    }

    /** Creates an AVRO field schema with data based on the given type. */
    private static Object generateAvroFieldData(Schema schema) {
        final org.apache.avro.LogicalType avroLogicalType = schema.getLogicalType();
        switch (schema.getType()) {
            case RECORD:
                GenericRecord record = new GenericData.Record(schema);
                for (Schema.Field field : schema.getFields()) {
                    record.put(field.name(), generateAvroFieldData(field.schema()));
                }
                return record;
            case DOUBLE:
                return 1.0;
            case LONG:
                if (avroLogicalType != null
                        && avroLogicalType
                                .getName()
                                .equals(LogicalTypes.timestampMillis().getName())) {
                    return 10L;
                } else {
                    return 1L;
                }
            case INT:
                return 1;
            case BOOLEAN:
                return true;
            case FLOAT:
                return 1.0f;
            case BYTES:
                if (avroLogicalType != null
                        && avroLogicalType.getName().equals(LogicalTypes.decimal(6).getName())) {
                    return ByteBuffer.wrap(new BigDecimal("100.001").unscaledValue().toByteArray());
                } else {
                    return ByteBuffer.wrap(new byte[] {0, 1});
                }
            case STRING:
                return new Utf8("");
            case ARRAY:
                final List<Object> array =
                        Arrays.asList(generateAvroFieldData(schema.getElementType()));
                return new GenericData.Array(schema, array);
            case MAP:
                final Map<Object, Object> map = new HashMap<>();
                map.put("", generateAvroFieldData(schema.getValueType()));
                return map;
            case ENUM:
                return new GenericData.EnumSymbol(schema, "red");
            default:
                // handle primitive types
                throw new UnsupportedOperationException("Unsupported schema: " + schema);
        }
    }
}
