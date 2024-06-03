/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.avro;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;

/** A converter from {@link Schema} to {@link LogicalType}. */
@Confluent
public class AvroToFlinkSchemaConverter {

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
                throw new ValidationException("Cyclic schemas are not supported.");
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
        final String type = schema.getProp(CommonConstants.CONNECT_TYPE_PROP);
        final String logicalType = schema.getProp(CommonConstants.AVRO_LOGICAL_TYPE_PROP);

        switch (schema.getType()) {
            case BOOLEAN:
                return new BooleanType(isOptional);
            case BYTES:
            case FIXED:
                if (CommonConstants.AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
                    final Object scaleNode =
                            schema.getObjectProp(CommonConstants.AVRO_LOGICAL_DECIMAL_SCALE_PROP);
                    // In Avro the scale is optional
                    final int scale =
                            scaleNode instanceof Number
                                    ? ((Number) scaleNode).intValue()
                                    : DecimalType.DEFAULT_SCALE;

                    Object precisionNode =
                            schema.getObjectProp(
                                    CommonConstants.AVRO_LOGICAL_DECIMAL_PRECISION_PROP);
                    final int precision;
                    if (null != precisionNode) {
                        if (!(precisionNode instanceof Number)) {
                            throw new ValidationException(
                                    CommonConstants.AVRO_LOGICAL_DECIMAL_PRECISION_PROP
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
                return new DoubleType(isOptional);
            case FLOAT:
                return new FloatType(isOptional);
            case INT:
                // INT is used for Connect's INT8, INT16, and INT32
                if (type == null && logicalType == null) {
                    return new IntType(isOptional);
                } else if (logicalType != null) {
                    if (CommonConstants.AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
                        return new DateType(isOptional);
                    } else if (CommonConstants.AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(
                            logicalType)) {
                        return createTimeType(schema, isOptional);
                    } else {
                        return new IntType(isOptional);
                    }
                } else if (type.equalsIgnoreCase("int8")) {
                    return new TinyIntType(isOptional);
                } else if (type.equalsIgnoreCase("int16")) {
                    return new SmallIntType(isOptional);
                } else {
                    throw new ValidationException(
                            "Connect type annotation for Avro int field is null");
                }
            case LONG:
                return createTimestampType(schema, isOptional)
                        .orElseGet(
                                () -> {
                                    if (CommonConstants.AVRO_LOGICAL_TIME_MICROS.equalsIgnoreCase(
                                            logicalType)) {
                                        // TODO we support only precision of 3 in Flink runtime,
                                        // because we store
                                        // time as int representing millis of day
                                        // return new TimeType(isOptional, 6);
                                        return new BigIntType(isOptional);
                                    } else {
                                        return new BigIntType(isOptional);
                                    }
                                });
            case STRING:
            case ENUM:
                // enums are unwrapped to strings and the original enum is not preserved
                return new VarCharType(isOptional, VarCharType.MAX_LENGTH);

            case ARRAY:
                Schema elemSchema = schema.getElementType();
                // Special case for custom encoding of non-string maps as list of key-value records
                if (isMapEntry(elemSchema)) {
                    if (elemSchema.getFields().size() != 2
                            || elemSchema.getField(CommonConstants.KEY_FIELD) == null
                            || elemSchema.getField(CommonConstants.VALUE_FIELD) == null) {
                        throw new ValidationException(
                                "Found map encoded as array of key-value pairs, but array "
                                        + "elements do not match the expected format.");
                    }
                    return new MapType(
                            isOptional,
                            toFlinkSchemaWithCycleDetection(
                                    elemSchema.getField(CommonConstants.KEY_FIELD).schema(),
                                    false,
                                    cycleContext),
                            toFlinkSchemaWithCycleDetection(
                                    elemSchema.getField(CommonConstants.VALUE_FIELD).schema(),
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
                        new VarCharType(false, VarCharType.MAX_LENGTH),
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

            case UNION:
                List<Schema> unionTypes = schema.getTypes();
                List<Schema> memberSchemas =
                        unionTypes.stream()
                                .filter(s -> s.getType() != NULL)
                                .collect(Collectors.toList());
                boolean isNullable = unionTypes.size() != memberSchemas.size();

                // Don't wrap it in a Row if there is only one non-NULL type
                if (memberSchemas.size() == 1) {
                    return toFlinkSchemaWithCycleDetection(
                            memberSchemas.get(0), isNullable, cycleContext);
                }

                List<UnionMember> unionMembers = new ArrayList<>();
                for (Schema memberSchema : memberSchemas) {

                    LogicalType memberType =
                            toFlinkSchemaWithCycleDetection(memberSchema, true, cycleContext);

                    unionMembers.add(
                            new UnionMember(
                                    memberSchema.getName(),
                                    memberSchema.getFullName(),
                                    memberType));
                }

                final Map<String, Long> simpleNameFreq =
                        unionMembers.stream()
                                .collect(
                                        Collectors.groupingBy(
                                                UnionMember::getSimpleName, Collectors.counting()));

                Set<String> fieldNames = new HashSet<>();
                List<RowField> unionFields =
                        unionMembers.stream()
                                .map(
                                        member -> {
                                            final String fieldName =
                                                    simpleNameFreq.get(member.getSimpleName()) == 1
                                                            ? member.getSimpleName()
                                                            : member.getFullName();
                                            if (!fieldNames.add(fieldName)) {
                                                throw new ValidationException(
                                                        "Multiple union schemas map to the same union field name");
                                            }
                                            return new RowField(fieldName, member.getLogicalType());
                                        })
                                .collect(Collectors.toList());

                return new RowType(isNullable, unionFields);

            case NULL:
                return new NullType();

            default:
                throw new ValidationException(
                        "Couldn't translate unsupported Avro schema type "
                                + schema.getType().getName()
                                + ".");
        }
    }

    private static TimeType createTimeType(Schema schema, boolean isOptional) {
        final int precision =
                Optional.ofNullable(schema.getObjectProp(CommonConstants.FLINK_PRECISION))
                        .map(i -> (Integer) i)
                        .orElse(3);
        if (precision > 3) {
            throw new ValidationException(
                    String.format("Illegal precision %d for TIME type.", precision));
        }
        return new TimeType(isOptional, precision);
    }

    private static Optional<LogicalType> createTimestampType(Schema schema, boolean isOptional) {
        final Optional<Integer> propertyPrecision =
                Optional.ofNullable(schema.getObjectProp(CommonConstants.FLINK_PRECISION))
                        .map(i -> (Integer) i);

        final String logicalType = schema.getProp(CommonConstants.AVRO_LOGICAL_TYPE_PROP);
        if (CommonConstants.AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
            final int precision = propertyPrecision.orElse(3);
            if (precision > 3) {
                throw new ValidationException(
                        String.format("Illegal precision %d for timestamp millis.", precision));
            }
            return Optional.of(new LocalZonedTimestampType(isOptional, precision));
        } else if (CommonConstants.AVRO_LOGICAL_TIMESTAMP_MICROS.equalsIgnoreCase(logicalType)) {
            final int precision = propertyPrecision.orElse(6);
            if (precision > 6) {
                throw new ValidationException(
                        String.format("Illegal precision %d for timestamp micros.", precision));
            }
            return Optional.of(new LocalZonedTimestampType(isOptional, precision));
        } else if (CommonConstants.AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS.equalsIgnoreCase(
                logicalType)) {
            final int precision = propertyPrecision.orElse(3);
            if (precision > 3) {
                throw new ValidationException(
                        String.format(
                                "Illegal precision %d for local timestamp millis.", precision));
            }
            return Optional.of(new TimestampType(isOptional, precision));
        } else if (CommonConstants.AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS.equalsIgnoreCase(
                logicalType)) {
            final int precision = propertyPrecision.orElse(6);
            if (precision > 6) {
                throw new ValidationException(
                        String.format(
                                "Illegal precision %d for local timestamp micros.", precision));
            }
            return Optional.of(new TimestampType(isOptional, precision));
        }

        return Optional.empty();
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

    private static final class UnionMember {
        private final String simpleName;
        private final String fullName;
        private final LogicalType logicalType;

        private UnionMember(String simpleName, String fullName, LogicalType logicalType) {
            this.simpleName = simpleName;
            this.fullName = fullName.replace('.', '_');
            this.logicalType = logicalType;
        }

        public String getSimpleName() {
            return simpleName;
        }

        public String getFullName() {
            return fullName;
        }

        public LogicalType getLogicalType() {
            return logicalType;
        }
    }
}
