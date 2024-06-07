/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.protobuf;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.ValidationException;
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
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.OneofDescriptor;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A converter from {@link Descriptor} to {@link LogicalType}.
 *
 * <p>The mapping is represented by the following table:
 *
 * <pre>
 * +-----------------------------------------------------+-----------------------------------------------------+-------------------------+-------------------------------------------------------+
 * |                    Protobuf type                    |                    Message type                     | Connect type annotation |                      Flink type                       |
 * +-----------------------------------------------------+-----------------------------------------------------+-------------------------+-------------------------------------------------------+
 * | INT32|SINT32|SFIXED32                               | -                                                   |                         | INT                                                   |
 * | INT32|SINT32|SFIXED32                               | -                                                   | int8                    | TINYINT                                               |
 * | INT32|SINT32|SFIXED32                               | -                                                   | int16                   | SMALLINT                                              |
 * | UINT32|FIXED32|INT64|UINT64|SINT64|FIXED64|SFIXED64 | -                                                   | -                       | BIGINT                                                |
 * | FLOAT                                               | -                                                   | -                       | FLOAT                                                 |
 * | DOUBLE                                              | -                                                   | -                       | DOUBLE                                                |
 * | BOOL                                                | -                                                   | -                       | BOOLEAN                                               |
 * | ENUM                                                | -                                                   | -                       | VARCHAR                                               |
 * | STRING                                              | -                                                   | -                       | VARCHAR                                               |
 * | BYTES                                               | -                                                   | -                       | VARBINARY                                             |
 * | MESSAGE                                             | confluent.type.Decimal                              | -                       | DECIMAL                                               |
 * | MESSAGE                                             | google.type.Date                                    | -                       | DATE                                                  |
 * | MESSAGE                                             | google.type.TimeOfDay                               | -                       | TIME(3) // Flink does not support higher precision    |
 * | MESSAGE                                             | google.protobuf.Timestamp                           | -                       | TIMESTAMP_LTZ(9)                                      |
 * | MESSAGE                                             | google.protobuf.DoubleValue                         | -                       | DOUBLE                                                |
 * | MESSAGE                                             | google.protobuf.FloatValue                          | -                       | FLOAT                                                 |
 * | MESSAGE                                             | google.protobuf.Int32Value                          | -                       | INT                                                   |
 * | MESSAGE                                             | google.protobuf.Int64Value                          | -                       | BIGINT                                                |
 * | MESSAGE                                             | google.protobuf.UInt64Value                         | -                       | BIGINT                                                |
 * | MESSAGE                                             | google.protobuf.UInt32Value                         | -                       | BIGINT                                                |
 * | MESSAGE                                             | google.protobuf.BoolValue                           | -                       | BOOLEAN                                               |
 * | MESSAGE                                             | google.protobuf.StringValue                         | -                       | VARCHAR                                               |
 * | MESSAGE                                             | google.protobuf.BytesValue                          | -                       | VARBINARY                                             |
 * | MESSAGE                                             | repeated xx.xx.XXEntry([type1] key, [type2] value)  | -                       | MAP[type1, type2]                                     |
 * | MESSAGE                                             | -                                                   | -                       | ROW                                                   |
 * | oneOf                                               | -                                                   | -                       | ROW                                                   |
 * +-----------------------------------------------------+-----------------------------------------------------+-------------------------+-------------------------------------------------------+
 * </pre>
 *
 * <p>Expressing something as NULLABLE or NOT NULL is not straightforward in Protobuf.
 *
 * <ul>
 *   <li>all non MESSAGE types are NOT NULL (if not set explicitly default value is assigned)
 *   <li>non MESSAGE types marked with 'optional' can be checked if they were set. If not set we
 *       assume NULL
 *   <li>MESSAGE types are all NULLABLE, in other words all fields of a MESSAGE type are optional
 *       and there is no way to ensure on a format level they are NOT NULL
 *   <li>ARRAYS can not be NULL, not set repeated field is presented as an empty list, there is no
 *       way to differentiate an empty array from NULL
 * </ul>
 */
@Confluent
public class ProtoToFlinkSchemaConverter {

    /**
     * Mostly adapted the logic from <a
     * href="https://github.com/confluentinc/schema-registry/blob/610fbed58a3a8d778ec7a9de5b8d2d0c1465c6f9/protobuf-converter/src/main/java/io/confluent/connect/protobuf/ProtobufData.java">ProtobufData</a>
     * Should be kept in sync to handle all connect data types.
     */
    public static LogicalType toFlinkSchema(final Descriptor schema) {
        final CycleContext context = new CycleContext();
        // top-level row must not be NULLABLE in SQL, thus we change the nullability of the top row
        return toFlinkSchemaNested(false, schema, context);
    }

    private static LogicalType toFlinkSchemaNested(
            boolean isOptional, final Descriptor schema, final CycleContext context) {
        List<OneofDescriptor> oneOfDescriptors = schema.getRealOneofs();
        final List<RowField> fields = new ArrayList<>();
        final List<RowField> oneOfFields = new ArrayList<>();
        for (OneofDescriptor oneOfDescriptor : oneOfDescriptors) {
            oneOfFields.add(
                    new RowField(
                            oneOfDescriptor.getName(), toFlinkSchema(oneOfDescriptor, context)));
        }
        List<FieldDescriptor> fieldDescriptors = schema.getFields();
        for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
            OneofDescriptor oneOfDescriptor = fieldDescriptor.getRealContainingOneof();
            if (oneOfDescriptor != null) {
                // Already added field as oneof
                continue;
            }
            fields.add(
                    new RowField(
                            fieldDescriptor.getName(), toFlinkSchema(fieldDescriptor, context)));
        }
        fields.addAll(oneOfFields);
        return new RowType(isOptional, fields);
    }

    private static LogicalType toFlinkSchema(
            OneofDescriptor oneOfDescriptor, CycleContext context) {
        List<FieldDescriptor> fieldDescriptors = oneOfDescriptor.getFields();
        final List<RowField> fields =
                fieldDescriptors.stream()
                        .map(field -> new RowField(field.getName(), toFlinkSchema(field, context)))
                        .collect(Collectors.toList());
        return new RowType(true, fields);
    }

    private static LogicalType toFlinkSchema(
            final FieldDescriptor schema, final CycleContext context) {
        boolean isNullableType =
                getMeta(schema)
                        .flatMap(getParam(CommonConstants.FLINK_NOT_NULL))
                        .map(s -> !Boolean.parseBoolean(s))
                        // message types are nullable in PROTOBUF, native types are not null
                        .orElseGet(
                                () ->
                                        schema.getType().equals(Type.MESSAGE)
                                                && !schema.isRepeated());
        if (schema.isRepeated()) {
            return convertRepeated(schema, context, isNullableType);
        } else {
            return convertNonRepeated(schema, context, isNullableType);
        }
    }

    private static LogicalType convertNonRepeated(
            FieldDescriptor schema, CycleContext cycleContext, boolean isNullableType) {
        final boolean isOptional = schema.hasOptionalKeyword() || isNullableType;

        switch (schema.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
                {
                    if (schema.getOptions().hasExtension(MetaProto.fieldMeta)) {
                        Meta fieldMeta = schema.getOptions().getExtension(MetaProto.fieldMeta);
                        Map<String, String> params = fieldMeta.getParamsMap();
                        String connectType = params.get(CommonConstants.CONNECT_TYPE_PROP);
                        if (CommonConstants.CONNECT_TYPE_INT8.equals(connectType)) {
                            return new TinyIntType(isOptional);
                        } else if (CommonConstants.CONNECT_TYPE_INT16.equals(connectType)) {
                            return new SmallIntType(isOptional);
                        }
                    }
                    return new IntType(isOptional);
                }
            case UINT32:
            case FIXED32:
            case INT64:
            case UINT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                return new BigIntType(isOptional);
            case FLOAT:
                return new FloatType(isOptional);
            case DOUBLE:
                return new DoubleType(isOptional);
            case BOOL:
                return new BooleanType(isOptional);
            case ENUM:
                return new VarCharType(isOptional, VarCharType.MAX_LENGTH);
            case STRING:
                return createStringType(isOptional, schema, CharType::new, VarCharType::new);
            case BYTES:
                return createStringType(isOptional, schema, BinaryType::new, VarBinaryType::new);
            case MESSAGE:
                {
                    String fullName = schema.getMessageType().getFullName();
                    switch (fullName) {
                        case CommonConstants.PROTOBUF_DECIMAL_TYPE:
                            return createDecimalType(isOptional, schema);
                        case CommonConstants.PROTOBUF_DATE_TYPE:
                            return new DateType(isOptional);
                        case CommonConstants.PROTOBUF_TIME_TYPE:
                            return createTimeType(isOptional, schema);
                        case CommonConstants.PROTOBUF_TIMESTAMP_TYPE:
                            return createTimestampType(isOptional, schema);
                        default:
                            if (cycleContext.seenMessage.contains(fullName)) {
                                throw new ValidationException("Cyclic schemas are not supported.");
                            }
                            cycleContext.seenMessage.add(fullName);
                            final LogicalType recordSchema =
                                    toUnwrappedOrRecordSchema(isOptional, schema, cycleContext);
                            cycleContext.seenMessage.remove(fullName);
                            return recordSchema;
                    }
                }
            default:
                throw new ValidationException("Unknown Protobuf schema type " + schema.getType());
        }
    }

    /** Utility to interface just to name the parameters. */
    @FunctionalInterface
    private interface LengthLimitedTypeConstructor {
        LogicalType create(boolean isOptional, int length);
    }

    private static LogicalType createStringType(
            boolean isOptional,
            FieldDescriptor schema,
            LengthLimitedTypeConstructor constantTypeCtor,
            LengthLimitedTypeConstructor variableTypeCtor) {
        final Optional<Meta> meta = getMeta(schema);
        if (meta.isPresent()) {
            final Meta fieldMeta = meta.get();
            final int minLength =
                    Integer.parseInt(
                            fieldMeta.getParamsOrDefault(CommonConstants.FLINK_MIN_LENGTH, "-1"));
            final int maxLength =
                    Optional.ofNullable(
                                    fieldMeta.getParamsOrDefault(
                                            CommonConstants.FLINK_MAX_LENGTH, null))
                            .map(Integer::valueOf)
                            .orElse(VarCharType.MAX_LENGTH);
            if (minLength > 0 && minLength == maxLength) {
                return constantTypeCtor.create(isOptional, maxLength);
            } else {
                return variableTypeCtor.create(isOptional, maxLength);
            }
        } else {
            return variableTypeCtor.create(isOptional, VarCharType.MAX_LENGTH);
        }
    }

    private static TimeType createTimeType(boolean isOptional, FieldDescriptor schema) {
        final int defaultPrecision = 3;
        final int precision =
                getMeta(schema)
                        .map(
                                m ->
                                        Integer.parseInt(
                                                m.getParamsOrDefault(
                                                        CommonConstants.FLINK_PRECISION_PROP,
                                                        String.valueOf(defaultPrecision))))
                        .orElse(defaultPrecision);
        if (precision > 3) {
            throw new ValidationException(
                    "Flink does not support TIME type with precision "
                            + precision
                            + ", it only supports precision less than or equal to 3.");
        }
        return new TimeType(isOptional, precision);
    }

    private static DecimalType createDecimalType(boolean isOptional, FieldDescriptor schema) {
        int precision = DecimalType.DEFAULT_PRECISION;
        int scale = DecimalType.DEFAULT_SCALE;
        final Optional<Meta> meta = getMeta(schema);
        if (meta.isPresent()) {
            Meta fieldMeta = meta.get();
            Map<String, String> params = fieldMeta.getParamsMap();
            String precisionStr = params.get(CommonConstants.PROTOBUF_PRECISION_PROP);
            if (precisionStr != null) {
                try {
                    precision = Integer.parseInt(precisionStr);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            String scaleStr = params.get(CommonConstants.PROTOBUF_SCALE_PROP);
            if (scaleStr != null) {
                try {
                    scale = Integer.parseInt(scaleStr);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
        return new DecimalType(isOptional, precision, scale);
    }

    private static LogicalType createTimestampType(boolean isOptional, FieldDescriptor schema) {
        final int defaultPrecision = 9;
        final Optional<Meta> meta = getMeta(schema);
        if (meta.isPresent()) {
            final int precision =
                    meta.map(
                                    m ->
                                            Integer.parseInt(
                                                    m.getParamsOrDefault(
                                                            CommonConstants.FLINK_PRECISION_PROP,
                                                            String.valueOf(defaultPrecision))))
                            .orElse(defaultPrecision);

            if (CommonConstants.FLINK_TYPE_TIMESTAMP.equals(
                    meta.get().getParamsOrDefault(CommonConstants.FLINK_TYPE_PROP, null))) {
                return new TimestampType(isOptional, precision);
            } else {
                return new LocalZonedTimestampType(isOptional, precision);
            }
        } else {
            return new LocalZonedTimestampType(isOptional, defaultPrecision);
        }
    }

    private static Optional<Meta> getMeta(FieldDescriptor schema) {
        if (schema.getOptions().hasExtension(MetaProto.fieldMeta)) {
            return Optional.of(schema.getOptions().getExtension(MetaProto.fieldMeta));
        }
        return Optional.empty();
    }

    private static LogicalType convertRepeated(
            FieldDescriptor schema, CycleContext context, boolean isNullableType) {
        if (isMapDescriptor(schema)) {
            return toMapSchema(schema.getMessageType(), isNullableType, context);
        } else {
            final boolean isArrayElementWrapped =
                    getMeta(schema)
                            .flatMap(getParam(CommonConstants.FLINK_WRAPPER))
                            .map(Boolean::parseBoolean)
                            // repeated types are not null in PROTOBUF, if no elements were set we
                            // will get an empty list
                            .orElse(false);
            if (isArrayElementWrapped) {
                // there should be a single field in the wrapper
                final FieldDescriptor elementSchema = schema.getMessageType().getFields().get(0);
                return new ArrayType(isNullableType, toFlinkSchema(elementSchema, context));
            } else {
                return new ArrayType(isNullableType, convertNonRepeated(schema, context, false));
            }
        }
    }

    private static Function<Meta, Optional<String>> getParam(String paramKey) {
        return m -> Optional.ofNullable(m.getParamsOrDefault(paramKey, null));
    }

    private static LogicalType toUnwrappedOrRecordSchema(
            boolean isOptional, FieldDescriptor descriptor, CycleContext context) {
        final boolean isRepeatedWrapped =
                getMeta(descriptor)
                        .flatMap(getParam(CommonConstants.FLINK_WRAPPER))
                        .map(Boolean::parseBoolean)
                        // repeated types are not null in PROTOBUF
                        .orElse(false);
        if (isRepeatedWrapped) {
            final FieldDescriptor elementSchema = descriptor.getMessageType().getFields().get(0);
            return convertRepeated(elementSchema, context, true);
        }

        return toUnwrappedSchema(descriptor.getMessageType())
                .orElseGet(
                        () ->
                                toFlinkSchemaNested(
                                        isOptional, descriptor.getMessageType(), context));
    }

    private static Optional<LogicalType> toUnwrappedSchema(Descriptor descriptor) {
        String fullName = descriptor.getFullName();
        switch (fullName) {
            case CommonConstants.PROTOBUF_DOUBLE_WRAPPER_TYPE:
                return Optional.of(new DoubleType(true));
            case CommonConstants.PROTOBUF_FLOAT_WRAPPER_TYPE:
                return Optional.of(new FloatType(true));
            case CommonConstants.PROTOBUF_INT64_WRAPPER_TYPE:
            case CommonConstants.PROTOBUF_UINT64_WRAPPER_TYPE:
            case CommonConstants.PROTOBUF_UINT32_WRAPPER_TYPE:
                return Optional.of(new BigIntType(true));
            case CommonConstants.PROTOBUF_INT32_WRAPPER_TYPE:
                return Optional.of(new IntType(true));
            case CommonConstants.PROTOBUF_BOOL_WRAPPER_TYPE:
                return Optional.of(new BooleanType(true));
            case CommonConstants.PROTOBUF_STRING_WRAPPER_TYPE:
                return Optional.of(new VarCharType(true, VarCharType.MAX_LENGTH));
            case CommonConstants.PROTOBUF_BYTES_WRAPPER_TYPE:
                return Optional.of(new VarBinaryType(true, VarBinaryType.MAX_LENGTH));
            default:
                return Optional.empty();
        }
    }

    private static LogicalType toMapSchema(
            Descriptor descriptor, boolean isNullableType, CycleContext context) {
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
        return new MapType(
                isNullableType,
                toFlinkSchema(fieldDescriptors.get(0), context),
                toFlinkSchema(fieldDescriptors.get(1), context));
    }

    private static boolean isMapDescriptor(FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getType() != Type.MESSAGE) {
            return false;
        }
        Descriptor descriptor = fieldDescriptor.getMessageType();
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
        return descriptor.getName().endsWith(CommonConstants.MAP_ENTRY_SUFFIX)
                && fieldDescriptors.size() == 2
                && fieldDescriptors.get(0).getName().equals(CommonConstants.KEY_FIELD)
                && fieldDescriptors.get(1).getName().equals(CommonConstants.VALUE_FIELD);
    }

    private static final class CycleContext {
        private final Set<String> seenMessage = new HashSet<>();
    }
}
