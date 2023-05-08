/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry.converters;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** A converter from {@link Schema} to {@link LogicalType}. */
@Confluent
public class AvroToFlinkSchemaConverter {

    private static final String CONNECT_TYPE_PROP = "connect.type";
    private static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
    private static final String AVRO_LOGICAL_DECIMAL = "decimal";
    private static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
    private static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
    private static final String AVRO_LOGICAL_DATE = "date";
    private static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
    private static final String AVRO_LOGICAL_TIME_MICROS = "time-micros";
    private static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String AVRO_LOGICAL_TIMESTAMP_MICROS = "timestamp-micros";
    private static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
    private static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final Schema NULL_AVRO_SCHEMA = Schema.create(Type.NULL);

    /**
     * Mostly copied over from <a
     * href="https://github.com/confluentinc/schema-registry/blob/5eee929eb51cee64dd021897943d2195db722efa/avro-data/src/main/java/io/confluent/connect/avro/AvroData.java#L1734">AvroData</a>
     * Should be kept in sync to handle all connect data types.
     */
    public static LogicalType toFlinkSchema(final Schema schema) {
        final CycleContext context = new CycleContext();
        return toFlinkSchemaWithCycleDetection(schema, false, context);
    }

    private static LogicalType toFlinkSchemaWithCycleDetection(
            final Schema schema, boolean isOptional, CycleContext cycleContext) {

        final boolean isNamedType =
                schema.getType() == Type.UNION || schema.getType() == Type.RECORD;
        if (isNamedType) {
            if (cycleContext.seenSchemas.contains(schema)) {
                throw new IllegalArgumentException("Cyclic schemas are not supported.");
            }
            cycleContext.seenSchemas.add(schema);
        }
        final LogicalType result = toFlinkSchema(schema, isOptional, cycleContext);
        if (isNamedType) {
            cycleContext.seenSchemas.remove(schema);
        }
        return result;
    }

    private static LogicalType toFlinkSchema(
            final Schema schema, boolean isOptional, CycleContext cycleContext) {
        final String type = schema.getProp(CONNECT_TYPE_PROP);
        final String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);

        switch (schema.getType()) {
            case BOOLEAN:
                return new BooleanType(isOptional);
            case BYTES:
            case FIXED:
                if (AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
                    final Object scaleNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP);
                    // In Avro the scale is optional
                    final int scale =
                            scaleNode instanceof Number
                                    ? ((Number) scaleNode).intValue()
                                    : DecimalType.DEFAULT_SCALE;

                    Object precisionNode =
                            schema.getObjectProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP);
                    final int precision;
                    if (null != precisionNode) {
                        if (!(precisionNode instanceof Number)) {
                            throw new IllegalStateException(
                                    AVRO_LOGICAL_DECIMAL_PRECISION_PROP
                                            + " property must be a JSON Integer."
                                            + " https://avro.apache.org/docs/1.9.1/spec.html#Decimal");
                        }
                        // Capture the precision as a parameter only if it is not the default
                        precision = ((Number) precisionNode).intValue();
                    } else {
                        precision = DecimalType.DEFAULT_PRECISION;
                    }
                    return new DecimalType(isOptional, precision, scale);
                } else if (schema.getType() == Schema.Type.FIXED) {
                    return new VarBinaryType(isOptional, schema.getFixedSize());
                } else {
                    return new VarBinaryType(isOptional, VarBinaryType.MAX_LENGTH);
                }
            case DOUBLE:
                return new BigIntType(isOptional);
            case FLOAT:
                return new FloatType(isOptional);
            case INT:
                // INT is used for Connect's INT8, INT16, and INT32
                if (type == null && logicalType == null) {
                    return new IntType(isOptional);
                } else if (logicalType != null) {
                    if (AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
                        return new DateType(isOptional);
                    } else if (AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(logicalType)) {
                        return new TimeType(isOptional, 3);
                    } else {
                        return new IntType(isOptional);
                    }
                } else if (type.equalsIgnoreCase("int8")) {
                    return new TinyIntType(isOptional);
                } else if (type.equalsIgnoreCase("int16")) {
                    return new SmallIntType(isOptional);
                } else {
                    throw new IllegalStateException(
                            "Connect type annotation for Avro int field is null");
                }
            case LONG:
                if (AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
                    return new TimestampType(isOptional, 3);
                } else if (AVRO_LOGICAL_TIMESTAMP_MICROS.equalsIgnoreCase(logicalType)) {
                    return new TimestampType(isOptional, 6);
                } else if (AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
                    return new LocalZonedTimestampType(isOptional, 3);
                } else if (AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS.equalsIgnoreCase(logicalType)) {
                    return new LocalZonedTimestampType(isOptional, 6);
                } else if (AVRO_LOGICAL_TIME_MICROS.equalsIgnoreCase(logicalType)) {
                    // TODO we support only precision of 3 in Flink runtime, because we store
                    // time as int representing millis of day
                    // return new TimeType(isOptional, 6);
                    return new BigIntType(isOptional);
                } else {
                    return new BigIntType(isOptional);
                }
            case STRING:
                return new VarCharType(VarCharType.MAX_LENGTH);

            case ARRAY:
                Schema elemSchema = schema.getElementType();
                // Special case for custom encoding of non-string maps as list of key-value records
                if (isMapEntry(elemSchema)) {
                    if (elemSchema.getFields().size() != 2
                            || elemSchema.getField(KEY_FIELD) == null
                            || elemSchema.getField(VALUE_FIELD) == null) {
                        throw new IllegalStateException(
                                "Found map encoded as array of key-value pairs, but array "
                                        + "elements do not match the expected format.");
                    }
                    return new MapType(
                            isOptional,
                            toFlinkSchemaWithCycleDetection(
                                    elemSchema.getField(KEY_FIELD).schema(), false, cycleContext),
                            toFlinkSchemaWithCycleDetection(
                                    elemSchema.getField(VALUE_FIELD).schema(),
                                    false,
                                    cycleContext));
                } else {
                    return new ArrayType(
                            isOptional,
                            toFlinkSchemaWithCycleDetection(
                                    schema.getElementType(), false, cycleContext));
                }

            case MAP:
                return new MapType(
                        isOptional,
                        new VarCharType(VarCharType.MAX_LENGTH),
                        toFlinkSchemaWithCycleDetection(
                                schema.getValueType(), false, cycleContext));

            case RECORD:
                {
                    final List<RowField> rowFields =
                            schema.getFields().stream()
                                    .map(
                                            field ->
                                                    new RowField(
                                                            field.name(),
                                                            toFlinkSchemaWithCycleDetection(
                                                                    field.schema(),
                                                                    false,
                                                                    cycleContext)))
                                    .collect(Collectors.toList());
                    return new RowType(isOptional, rowFields);
                }

            case ENUM:
                // enums are unwrapped to strings and the original enum is not preserved
                return new VarCharType(VarCharType.MAX_LENGTH);

            case UNION:
                {
                    if (schema.getTypes().size() == 2) {
                        if (schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
                            for (Schema memberSchema : schema.getTypes()) {
                                if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
                                    return toFlinkSchemaWithCycleDetection(
                                            memberSchema, true, cycleContext);
                                }
                            }
                        }
                    }
                    // TODO: add runtime support for reading UNIONs first
                    throw new IllegalArgumentException("Custom UNION types are not yet supported.");
                    //                Set<String> fieldNames = new HashSet<>();
                    //                boolean unionOptional = false;
                    //                final List<RowField> rowFields = new ArrayList<>();
                    //                for (Schema memberSchema : schema.getTypes()) {
                    //                    if (memberSchema.getType() == Schema.Type.NULL) {
                    //                        unionOptional = true;
                    //                    } else {
                    //                        String fieldName = unionMemberFieldName(memberSchema);
                    //                        if (fieldNames.contains(fieldName)) {
                    //                            throw new IllegalStateException(
                    //                                    "Multiple union schemas map to the Connect
                    // union field name");
                    //                        }
                    //                        fieldNames.add(fieldName);
                    //                        rowFields.add(
                    //                                new RowField(
                    //                                        fieldName,
                    //                                        toFlinkSchemaWithCycleDetection(
                    //                                                memberSchema, false,
                    // cycleContext)));
                    //                    }
                    //                }
                    //                return new RowType(unionOptional || isOptional, rowFields);
                }

            case NULL:
                return new NullType();

            default:
                throw new IllegalStateException(
                        "Couldn't translate unsupported schema type "
                                + schema.getType().getName()
                                + ".");
        }
    }

    private static final class CycleContext {

        private final Set<Schema> seenSchemas = new HashSet<>();
    }

    private static boolean isMapEntry(Schema elemSchema) {
        if (!elemSchema.getType().equals(Type.RECORD)) {
            return false;
        } else if ("io.confluent.connect.avro".equals(elemSchema.getNamespace())
                && "MapEntry".equals(elemSchema.getName())) {
            return true;
        } else {
            return Objects.equals(elemSchema.getProp("connect.internal.type"), "MapEntry");
        }
    }

    private static String unionMemberFieldName(org.apache.avro.Schema schema) {
        if (schema.getType() != Type.RECORD
                && schema.getType() != Type.ENUM
                && schema.getType() != Type.FIXED) {
            return schema.getType().getName();
        } else {
            return schema.getFullName();
        }
    }
}
