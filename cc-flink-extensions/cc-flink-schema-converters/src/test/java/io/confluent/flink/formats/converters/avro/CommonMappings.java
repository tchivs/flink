/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.avro;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

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

    public static Stream<TypeMapping> get() {
        return Stream.of(
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
                new TypeMapping(nullable(SchemaBuilder.builder().doubleType()), new DoubleType()),
                new TypeMapping(nullable(SchemaBuilder.builder().longType()), new BigIntType()),
                new TypeMapping(nullable(SchemaBuilder.builder().intType()), new IntType()),
                new TypeMapping(nullable(SchemaBuilder.builder().booleanType()), new BooleanType()),
                new TypeMapping(nullable(SchemaBuilder.builder().floatType()), new FloatType()),
                new TypeMapping(
                        nullable(SchemaBuilder.builder().bytesType()),
                        new VarBinaryType(true, VarBinaryType.MAX_LENGTH)),
                new TypeMapping(
                        nullable(SchemaBuilder.builder().stringType()),
                        new VarCharType(true, VarCharType.MAX_LENGTH)));
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
