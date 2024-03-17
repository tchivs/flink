/*
 * Copyright 2024 Confluent Inc.
 */
package io.confluent.flink.formats.converters;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.flink.formats.converters.avro.CommonMappings;
import io.confluent.flink.formats.converters.avro.CommonMappings.TypeMapping;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.confluent.flink.formats.converters.avro.AvroToFlinkSchemaConverter.unionMemberFieldName;

/** Arguments provider for testing union types. */
public class UnionUtil {
    public static class DataProvider implements ArgumentsProvider {
        private static final Stream<TypeMapping> UNIONS =
                UnionUtil.SchemaProvider.createUnionTypeMappings();

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return UNIONS.map(DataProvider::withData);
        }

        public static Arguments withData(TypeMapping mapping) {
            // UNION_SCHEMA
            Schema unionSchema = mapping.getAvroSchema();

            // RECORD_SCHEMA[UNION_SCHEMA, UNION_SCHEMA]
            Schema avroSchema =
                    SchemaBuilder.record("topLevelRow")
                            .namespace("io.confluent.test")
                            .fields()
                            .name("union_type_1")
                            .type(unionSchema)
                            .noDefault()
                            .name("union_type_2")
                            .type(unionSchema)
                            .noDefault()
                            .endRecord();

            // UNION_TYPE_1, UNION_TYPE_2, ... FROM UNION_SCHEMA
            List<Schema> types = unionSchema.getTypes();

            // RECORD[FIELD[UNION_TYPE_1],FIELD[UNION_TYPE_2]]
            GenericRecord avroRecordWithUnionTypeFields =
                    new GenericRecordBuilder(avroSchema)
                            .set("union_type_1", generateAvroFieldData(types.get(0)))
                            .set("union_type_2", generateAvroFieldData(types.get(1)))
                            .build();

            // FLINK_SCHEMA
            List<LogicalType> flinkLogicalTypes = ((RowType) mapping.getFlinkType()).getChildren();

            GenericRowData expectedRowData = new GenericRowData(2);
            GenericRowData field1 = new GenericRowData(2);
            GenericRowData field2 = new GenericRowData(2);
            field1.setField(0, generateFlinkFieldData(flinkLogicalTypes.get(0)));
            field2.setField(1, generateFlinkFieldData(flinkLogicalTypes.get(1)));
            expectedRowData.setField(0, field1);
            expectedRowData.setField(1, field2);

            return Arguments.of(avroSchema, avroRecordWithUnionTypeFields, expectedRowData);
        }

        private static Object generateFlinkFieldData(LogicalType logicalType) {
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
                case VARCHAR:
                    return "";
                case DATE:
                    return Date.valueOf("1970-01-01");
                case TIME_WITHOUT_TIME_ZONE:
                    return Time.valueOf("00:00:00");
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return Timestamp.valueOf("1970-01-01 00:00:00");
                case DECIMAL:
                    return BigDecimal.ZERO;
                case BINARY:
                    Schema fixed = SchemaBuilder.fixed("fixed").size(3);
                    Schema fixedDecimal =
                            LogicalTypes.decimal(5, 3)
                                    .addToSchema(SchemaBuilder.fixed("fixedDecimal").size(5));
                    Schema decimal =
                            LogicalTypes.decimal(5, 3)
                                    .addToSchema(SchemaBuilder.builder().bytesType());

                    return SchemaBuilder.builder()
                            .record("top")
                            .namespace("io.confluent.test")
                            .fields()
                            .name("binary")
                            .type()
                            .bytesType()
                            .noDefault()
                            .name("fixed_binary")
                            .type(fixed)
                            .noDefault()
                            .name("fixed_decimal")
                            .type(fixedDecimal)
                            .noDefault()
                            .name("decimal")
                            .type(decimal)
                            .noDefault()
                            .endRecord();
                case ROW:
                    GenericRowData row = new GenericRowData(2);
                    GenericRowData field1 = new GenericRowData(2);
                    GenericRowData field2 = new GenericRowData(2);
                    field1.setField(0, generateFlinkFieldData(logicalType.getChildren().get(0)));
                    field2.setField(1, generateFlinkFieldData(logicalType.getChildren().get(1)));
                    return row;

                case ARRAY:
                case MULTISET:
                case MAP:

                case DISTINCT_TYPE:
                case STRUCTURED_TYPE:
                case RAW:
                case SYMBOL:
                case NULL:
                case CHAR:

                case VARBINARY:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_TIME:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported logical type: " + logicalType);
            }
        }

        /** Creates an AVRO field schema with data based on the given type. */
        private static Object generateAvroFieldData(Schema schema) {
            switch (schema.getType()) {
                case RECORD:
                    GenericRecord record = new GenericData.Record(schema);
                    for (Schema.Field field : schema.getFields()) {
                        record.put(field.name(), generateAvroFieldData(field.schema()));
                    }
                    return record;
                case ENUM:
                    return SchemaBuilder.enumeration("Enum").symbols("Symbol1", "Symbol2");
                case ARRAY:
                    return SchemaBuilder.array().items().stringType();
                case MAP:
                    return SchemaBuilder.map().values().stringType();
                case FIXED:
                    return SchemaBuilder.fixed("Fixed").size(3);
                case BYTES:
                    Schema fixed = SchemaBuilder.fixed("fixed").size(3);
                    Schema fixedDecimal =
                            LogicalTypes.decimal(5, 3)
                                    .addToSchema(SchemaBuilder.fixed("fixedDecimal").size(5));
                    Schema decimal =
                            LogicalTypes.decimal(5, 3)
                                    .addToSchema(SchemaBuilder.builder().bytesType());

                    return SchemaBuilder.builder()
                            .record("top")
                            .namespace("io.confluent.test")
                            .fields()
                            .name("binary")
                            .type()
                            .bytesType()
                            .noDefault()
                            .name("fixed_binary")
                            .type(fixed)
                            .noDefault()
                            .name("fixed_decimal")
                            .type(fixedDecimal)
                            .noDefault()
                            .name("decimal")
                            .type(decimal)
                            .noDefault()
                            .endRecord();
                default:
                    // handle primitive types
                    return schema;
            }
        }
    }

    /**
     * Creates a stream of type mappings of unions of 2 different types, and NULL added to the union
     * at positions 0, 1, and 2: (A, B) -> U(A, B), U(NULL, A,B), U(A, NULL, B), U(A, B, NULL).
     */
    public static class SchemaProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            Stream<TypeMapping> unionsWithoutNull = createUnionTypeMappings();
            Stream<TypeMapping> unionsWithNull =
                    IntStream.range(0, 3)
                            .mapToObj(
                                    i ->
                                            createUnionTypeMappings()
                                                    .map(
                                                            typeMapping ->
                                                                    addNullToUnion(typeMapping, i)))
                            .flatMap(Function.identity());

            return Stream.concat(unionsWithoutNull, unionsWithNull).map(Arguments::of);
        }

        /** Creates unions of all possible pairs of 2 different types. (A, B) -> U(A, B), U(B, A) */
        private static Stream<TypeMapping> createUnionTypeMappings() {
            return CommonMappings.getNotNull()
                    .flatMap(
                            first ->
                                    CommonMappings.getNotNull()
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
        private TypeMapping addNullToUnion(TypeMapping typeMapping, int position) {
            List<Schema> unionTypes = new ArrayList<>(typeMapping.getAvroSchema().getTypes());
            unionTypes.add(position, Schema.create(Schema.Type.NULL));

            return new TypeMapping(
                    Schema.createUnion(unionTypes),
                    // Row and fields are nullable
                    typeMapping.getFlinkType().copy(true));
        }
    }
}
