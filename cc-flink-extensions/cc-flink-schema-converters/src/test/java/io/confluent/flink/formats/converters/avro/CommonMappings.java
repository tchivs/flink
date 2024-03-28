/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.avro;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/** Common data to use in schema mapping tests. */
public final class CommonMappings {

    /** A mapping between corresponding Avro and Flink types. */
    public static class TypeMapping {

        private final Schema avroSchema;
        private final LogicalType flinkType;

        public TypeMapping(Schema avroSchema, LogicalType flinkType) {
            this.avroSchema = avroSchema;
            this.flinkType = flinkType;
        }

        public Schema getAvroSchema() {
            return avroSchema;
        }

        public LogicalType getFlinkType() {
            return flinkType;
        }

        @Override
        public String toString() {
            return "avroSchema=" + avroSchema + ", flinkType=" + flinkType;
        }
    }

    /** A {@link TypeMapping}, but additionally with example data. */
    public static class TypeMappingWithData {
        private final TypeMapping mapping;

        private final Object avroData;

        private final Object flinkData;

        public TypeMappingWithData(TypeMapping mapping, Object avroData, Object flinkData) {
            this.mapping = mapping;
            this.avroData = avroData;
            this.flinkData = flinkData;
        }

        public Schema getAvroSchema() {
            return mapping.getAvroSchema();
        }

        public LogicalType getFlinkType() {
            return mapping.getFlinkType();
        }

        public Object getAvroData() {
            return avroData;
        }

        public Object getFlinkData() {
            return flinkData;
        }

        @Override
        public String toString() {
            return "mapping=" + mapping;
        }
    }

    public static Stream<TypeMapping> get() {
        return getNotNull().stream()
                .flatMap(
                        t ->
                                Stream.of(
                                        t,
                                        new TypeMapping(
                                                nullable(t.getAvroSchema()),
                                                t.getFlinkType().copy(true))));
    }

    public static List<TypeMapping> getNotNull() {
        return Arrays.asList(
                new TypeMapping(SchemaBuilder.builder().doubleType(), new DoubleType(false)),
                new TypeMapping(SchemaBuilder.builder().longType(), new BigIntType(false)),
                new TypeMapping(SchemaBuilder.builder().intType(), new IntType(false)),
                new TypeMapping(SchemaBuilder.builder().booleanType(), new BooleanType(false)),
                new TypeMapping(SchemaBuilder.builder().floatType(), new FloatType(false)),
                new TypeMapping(
                        SchemaBuilder.builder().bytesType(),
                        new VarBinaryType(false, VarBinaryType.MAX_LENGTH)),
                new TypeMapping(
                        SchemaBuilder.builder().stringType(),
                        new VarCharType(false, VarCharType.MAX_LENGTH)),
                new TypeMapping(
                        SchemaBuilder.builder().array().items(SchemaBuilder.builder().longType()),
                        new ArrayType(false, new BigIntType(false))),
                new TypeMapping(
                        SchemaBuilder.array()
                                .items(
                                        connectCustomMapType(
                                                SchemaBuilder.builder().intType(),
                                                SchemaBuilder.builder().bytesType())),
                        new MapType(
                                false,
                                new IntType(false),
                                new VarBinaryType(false, VarBinaryType.MAX_LENGTH))),
                new TypeMapping(
                        SchemaBuilder.map().values(SchemaBuilder.builder().booleanType()),
                        new MapType(
                                false,
                                new VarCharType(false, VarCharType.MAX_LENGTH),
                                new BooleanType(false))),
                new TypeMapping(
                        LogicalTypes.timestampMillis()
                                .addToSchema(SchemaBuilder.builder().longType()),
                        new TimestampType(false, 3)),
                new TypeMapping(
                        LogicalTypes.decimal(6, 3).addToSchema(SchemaBuilder.builder().bytesType()),
                        new DecimalType(false, 6, 3)));
    }

    public static Schema nullable(Schema schema) {
        return SchemaBuilder.unionOf().nullType().and().type(schema).endUnion();
    }

    public static Schema connectCustomMapType(Schema keyType, Schema valueType) {
        return SchemaBuilder.record("MapEntry")
                .namespace("io.confluent.connect.avro")
                .fields()
                .name("key")
                .type(keyType)
                .noDefault()
                .name("value")
                .type(valueType)
                .noDefault()
                .endRecord();
    }

    private CommonMappings() {}
}
