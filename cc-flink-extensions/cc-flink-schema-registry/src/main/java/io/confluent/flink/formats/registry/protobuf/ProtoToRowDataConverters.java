/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.protobuf;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.utils.DateTimeUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import io.confluent.flink.formats.converters.protobuf.CommonConstants;
import io.confluent.protobuf.MetaProto;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Runtime converters between {@link com.google.protobuf.Message} and {@link
 * org.apache.flink.table.data.RowData}.
 */
@Confluent
public class ProtoToRowDataConverters {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    /**
     * Runtime converter that converts Protobuf data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface ProtoToRowDataConverter extends Serializable {

        Object convert(Object object) throws IOException;
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260).
    // --------------------------------------------------------------------------------

    /** Creates a runtime converter. */
    public static ProtoToRowDataConverter createConverter(
            Descriptor readSchema, RowType targetType) {
        if (readSchema.getRealOneofs().isEmpty()) {
            return createNoOneOfRowConverter(readSchema, targetType);
        } else {
            return createOneOfRowConverter(readSchema, targetType);
        }
    }

    private static ProtoToRowDataConverter createOneOfRowConverter(
            Descriptor readSchema, RowType targetType) {
        final Map<String, OneofDescriptor> oneOfDescriptors =
                readSchema.getRealOneofs().stream()
                        .collect(Collectors.toMap(OneofDescriptor::getName, Function.identity()));
        final Map<String, FieldDescriptor> fieldDescriptors =
                readSchema.getFields().stream()
                        .filter(fieldDescriptor -> fieldDescriptor.getRealContainingOneof() == null)
                        .collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));

        final int arity = targetType.getFieldCount();
        final List<OneOfDescriptorWithConverter> oneOfConverters =
                targetType.getFields().stream()
                        .filter(field -> oneOfDescriptors.containsKey(field.getName()))
                        .map(
                                rowField -> {
                                    final OneofDescriptor fieldDescriptor =
                                            oneOfDescriptors.get(rowField.getName());
                                    return new OneOfDescriptorWithConverter(
                                            fieldDescriptor,
                                            createConverter(
                                                    fieldDescriptor, (RowType) rowField.getType()));
                                })
                        .collect(Collectors.toList());
        final List<FieldDescriptorWithConverter> fieldConverters =
                targetType.getFields().stream()
                        .filter(rowField -> !oneOfDescriptors.containsKey(rowField.getName()))
                        .map(
                                rowField -> {
                                    final FieldDescriptor fieldDescriptor =
                                            fieldDescriptors.get(rowField.getName());
                                    return new FieldDescriptorWithConverter(
                                            fieldDescriptor,
                                            createFieldConverter(
                                                    fieldDescriptor, rowField.getType()));
                                })
                        .collect(Collectors.toList());
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                final GenericRowData row = new GenericRowData(arity);
                final Message message = (Message) object;
                int i = 0;
                for (OneOfDescriptorWithConverter descriptorWithConverter : oneOfConverters) {
                    final OneofDescriptor descriptor = descriptorWithConverter.descriptor;
                    final ProtoToRowDataConverter converter = descriptorWithConverter.converter;
                    if (message.hasOneof(descriptor)) {
                        row.setField(i, converter.convert(message));
                    }
                    i++;
                }
                for (FieldDescriptorWithConverter descriptorWithConverter : fieldConverters) {
                    final FieldDescriptor fieldDescriptor = descriptorWithConverter.descriptor;
                    final ProtoToRowDataConverter converter = descriptorWithConverter.converter;
                    if (!fieldDescriptor.hasPresence() || message.hasField(fieldDescriptor)) {
                        row.setField(i, converter.convert(message.getField(fieldDescriptor)));
                    }
                    i++;
                }

                return row;
            }
        };
    }

    private static ProtoToRowDataConverter createNoOneOfRowConverter(
            Descriptor readSchema, RowType targetType) {
        final Map<String, FieldDescriptor> fieldDescriptors =
                readSchema.getFields().stream()
                        .collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));

        final int arity = targetType.getFieldCount();
        final FieldDescriptorWithConverter[] fieldConverters =
                targetType.getFields().stream()
                        .map(
                                rowField -> {
                                    final FieldDescriptor fieldDescriptor =
                                            fieldDescriptors.get(rowField.getName());
                                    return new FieldDescriptorWithConverter(
                                            fieldDescriptor,
                                            createFieldConverter(
                                                    fieldDescriptor, rowField.getType()));
                                })
                        .toArray(FieldDescriptorWithConverter[]::new);
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                final GenericRowData row = new GenericRowData(arity);
                final Message message = (Message) object;
                for (int i = 0; i < arity; i++) {
                    final FieldDescriptor fieldDescriptor = fieldConverters[i].descriptor;
                    final ProtoToRowDataConverter converter = fieldConverters[i].converter;
                    if (!fieldDescriptor.hasPresence() || message.hasField(fieldDescriptor)) {
                        row.setField(i, converter.convert(message.getField(fieldDescriptor)));
                    }
                }

                return row;
            }
        };
    }

    private static class FieldDescriptorWithConverter {
        final FieldDescriptor descriptor;
        final ProtoToRowDataConverter converter;

        private FieldDescriptorWithConverter(
                FieldDescriptor descriptor, ProtoToRowDataConverter converter) {
            this.descriptor = descriptor;
            this.converter = converter;
        }
    }

    private static class OneOfDescriptorWithConverter {
        final OneofDescriptor descriptor;
        final ProtoToRowDataConverter converter;

        private OneOfDescriptorWithConverter(
                OneofDescriptor descriptor, ProtoToRowDataConverter converter) {
            this.descriptor = descriptor;
            this.converter = converter;
        }
    }

    private static ProtoToRowDataConverter createConverter(
            OneofDescriptor readSchema, RowType targetType) {
        final int arity = targetType.getFieldCount();
        final Map<FieldDescriptor, Pair<ProtoToRowDataConverter, Integer>> fieldConverters =
                new HashMap<>();
        for (int i = 0; i < targetType.getFieldCount(); i++) {
            final FieldDescriptor fieldDescriptor = readSchema.getField(i);
            fieldConverters.put(
                    fieldDescriptor,
                    Pair.of(createFieldConverter(fieldDescriptor, targetType.getTypeAt(i)), i));
        }
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                final Message message = (Message) object;
                final GenericRowData row = new GenericRowData(arity);
                final FieldDescriptor oneofFieldDescriptor =
                        message.getOneofFieldDescriptor(readSchema);
                final Pair<ProtoToRowDataConverter, Integer> converters =
                        fieldConverters.get(oneofFieldDescriptor);
                row.setField(
                        converters.getRight(),
                        converters.getLeft().convert(message.getField(oneofFieldDescriptor)));
                return row;
            }
        };
    }

    private static ProtoToRowDataConverter createFieldConverter(
            FieldDescriptor readSchema, LogicalType targetType) {
        final Type schemaType = readSchema.getType();
        switch (targetType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return createStringConverter(targetType, schemaType);
            case BOOLEAN:
                return createBooleanConverter(targetType, schemaType);
            case BINARY:
            case VARBINARY:
                return createBinaryConverter(targetType, schemaType);
            case TIME_WITHOUT_TIME_ZONE:
                return createTimeConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimestampConverter();
            case DATE:
                return createDateConverter();
            case DECIMAL:
                return createDecimalConverter((DecimalType) targetType);
            case TINYINT:
                return createTinyIntConverter(targetType, schemaType);
            case SMALLINT:
                return createSmallIntConverter(targetType, schemaType);
            case INTEGER:
                return createIntegerConverter(targetType, schemaType);
            case BIGINT:
                return createBigintConverter(targetType, schemaType);
            case FLOAT:
                return createFloatConverter(targetType, schemaType);
            case DOUBLE:
                return createDoubleConverter(targetType, schemaType);
            case ARRAY:
                final ArrayType arrayType = (ArrayType) targetType;
                if (arrayType.isNullable() && isRepeatedWrapped(readSchema)) {
                    final FieldDescriptor arraySchema =
                            readSchema.getMessageType().getFields().get(0);
                    final ProtoToRowDataConverter arrayConverter =
                            createArrayConverter(arraySchema, arrayType);
                    return createRepeatedWrapperConverter(arraySchema, arrayConverter);
                } else {
                    return createArrayConverter(readSchema, arrayType);
                }
            case MULTISET:
                final MultisetType multisetType = (MultisetType) targetType;
                if (multisetType.isNullable() && isRepeatedWrapped(readSchema)) {
                    final FieldDescriptor multisetSchema =
                            readSchema.getMessageType().getFields().get(0);
                    final ProtoToRowDataConverter multisetConverter =
                            createMultisetConverter(multisetSchema, multisetType);
                    return createRepeatedWrapperConverter(multisetSchema, multisetConverter);
                } else {
                    return createMultisetConverter(readSchema, multisetType);
                }
            case MAP:
                final MapType mapType = (MapType) targetType;
                if (mapType.isNullable() && isRepeatedWrapped(readSchema)) {
                    final FieldDescriptor mapSchema =
                            readSchema.getMessageType().getFields().get(0);
                    final ProtoToRowDataConverter mapConverter =
                            createMapConverter(mapSchema, mapType);
                    return createRepeatedWrapperConverter(mapSchema, mapConverter);
                } else {
                    return createMapConverter(readSchema, mapType);
                }
            case ROW:
                return createConverter(readSchema.getMessageType(), (RowType) targetType);
            case NULL:
            case RAW:
            case SYMBOL:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case TIMESTAMP_WITH_TIME_ZONE:
            case UNRESOLVED:
            default:
                throw new IllegalStateException(
                        "Couldn't translate unsupported type " + targetType.getTypeRoot() + ".");
        }
    }

    private static boolean isRepeatedWrapped(FieldDescriptor descriptor) {
        return Boolean.parseBoolean(
                descriptor
                        .getOptions()
                        .getExtension(MetaProto.fieldMeta)
                        .getParamsOrDefault(CommonConstants.FLINK_WRAPPER, "false"));
    }

    private static ProtoToRowDataConverter createRepeatedWrapperConverter(
            FieldDescriptor wrapperField, ProtoToRowDataConverter valueConverter) {
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                Message msg = (Message) object;
                if (wrapperField.isRepeated() || msg.hasField(wrapperField)) {
                    return valueConverter.convert(msg.getField(wrapperField));
                }
                return null;
            }
        };
    }

    private static ProtoToRowDataConverter createArrayConverter(
            FieldDescriptor readSchema, ArrayType targetType) {
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(targetType.getElementType());

        final boolean hasElementWrapper;
        if (readSchema.getOptions().hasExtension(MetaProto.fieldMeta)) {
            hasElementWrapper =
                    Boolean.parseBoolean(
                            readSchema
                                    .getOptions()
                                    .getExtension(MetaProto.fieldMeta)
                                    .getParamsOrDefault(CommonConstants.FLINK_WRAPPER, "false"));
        } else {
            hasElementWrapper = false;
        }
        if (hasElementWrapper) {
            return createArrayNullableElementsConverter(readSchema, targetType, elementClass);
        } else {
            return createArrayNonNullElementsConverter(readSchema, targetType, elementClass);
        }
    }

    private static ProtoToRowDataConverter createArrayNullableElementsConverter(
            FieldDescriptor readSchema, ArrayType targetType, Class<?> elementClass) {
        final FieldDescriptor elementField =
                readSchema
                        .getMessageType()
                        .findFieldByName(CommonConstants.FLINK_WRAPPER_FIELD_NAME);
        final ProtoToRowDataConverter elementConverter =
                createFieldConverter(elementField, targetType.getElementType());
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                final Collection<?> list = (Collection<?>) object;
                final int length = list.size();
                final Object[] array = (Object[]) Array.newInstance(elementClass, length);
                int i = 0;
                for (Object o : list) {
                    Message msg = (Message) o;
                    if (elementField.isRepeated() || msg.hasField(elementField)) {
                        array[i] = elementConverter.convert(msg.getField(elementField));
                    }
                    i++;
                }
                return new GenericArrayData(array);
            }
        };
    }

    private static ProtoToRowDataConverter createArrayNonNullElementsConverter(
            FieldDescriptor readSchema, ArrayType targetType, Class<?> elementClass) {
        final ProtoToRowDataConverter elementConverter =
                createFieldConverter(readSchema, targetType.getElementType());
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                final Collection<?> list = (Collection<?>) object;
                final int length = list.size();
                final Object[] array = (Object[]) Array.newInstance(elementClass, length);
                int i = 0;
                for (Object o : list) {
                    array[i] = elementConverter.convert(o);
                    i++;
                }
                return new GenericArrayData(array);
            }
        };
    }

    private static ProtoToRowDataConverter createMapConverter(
            FieldDescriptor readSchema, MapType targetType) {
        final FieldDescriptor keySchema = readSchema.getMessageType().findFieldByName(KEY_FIELD);
        final FieldDescriptor valueSchema =
                readSchema.getMessageType().findFieldByName(VALUE_FIELD);
        final ProtoToRowDataConverter keyConverter =
                createFieldConverter(keySchema, targetType.getKeyType());
        final ProtoToRowDataConverter valueConverter =
                createFieldConverter(valueSchema, targetType.getValueType());
        return createMapLikeConverter(keyConverter, valueConverter);
    }

    private static ProtoToRowDataConverter createMultisetConverter(
            FieldDescriptor readSchema, MultisetType targetType) {
        final FieldDescriptor keySchema = readSchema.getMessageType().findFieldByName(KEY_FIELD);
        final FieldDescriptor valueSchema =
                readSchema.getMessageType().findFieldByName(VALUE_FIELD);
        final ProtoToRowDataConverter keyConverter =
                createFieldConverter(keySchema, targetType.getElementType());
        final ProtoToRowDataConverter valueConverter =
                createFieldConverter(valueSchema, new IntType(false));
        return createMapLikeConverter(keyConverter, valueConverter);
    }

    @SuppressWarnings("unchecked")
    private static ProtoToRowDataConverter createMapLikeConverter(
            ProtoToRowDataConverter keyConverter, ProtoToRowDataConverter valueConverter) {
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                final Collection<? extends Message> protoMap =
                        (Collection<? extends Message>) object;
                final Map<Object, Object> map = new HashMap<>();
                for (Message message : protoMap) {
                    final Descriptor descriptor = message.getDescriptorForType();
                    final Object elemKey = message.getField(descriptor.findFieldByName(KEY_FIELD));
                    final Object elemValue =
                            message.getField(descriptor.findFieldByName(VALUE_FIELD));

                    final Object key = keyConverter.convert(elemKey);
                    final Object value = valueConverter.convert(elemValue);
                    map.put(key, value);
                }
                return new GenericMapData(map);
            }
        };
    }

    private static ProtoToRowDataConverter createStringConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.STRING || schemaType == Type.ENUM) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return StringData.fromString(object.toString());
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return StringData.fromString(extractValueField(object).toString());
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static ProtoToRowDataConverter createBooleanConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.BOOL) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return object;
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return extractValueField(object);
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static ProtoToRowDataConverter createBinaryConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.BYTES) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((ByteString) object).toByteArray();
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((ByteString) extractValueField(object)).toByteArray();
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static ProtoToRowDataConverter createTimeConverter() {
        return new ProtoToRowDataConverter() {

            @Override
            public Object convert(Object object) {
                final Message message = (Message) object;
                int hours = 0;
                int minutes = 0;
                int seconds = 0;
                int nanos = 0;
                for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
                    if (entry.getKey().getName().equals("hours")) {
                        hours = ((Number) entry.getValue()).intValue();
                    } else if (entry.getKey().getName().equals("minutes")) {
                        minutes = ((Number) entry.getValue()).intValue();
                    } else if (entry.getKey().getName().equals("seconds")) {
                        seconds = ((Number) entry.getValue()).intValue();
                    } else if (entry.getKey().getName().equals("nanos")) {
                        nanos = ((Number) entry.getValue()).intValue();
                    }
                }

                return hours * 3600000 + minutes * 60000 + seconds * 1000 + nanos / 1000_000;
            }
        };
    }

    private static ProtoToRowDataConverter createTinyIntConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.INT32 || schemaType == Type.SINT32 || schemaType == Type.SFIXED32) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) object).byteValue();
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) extractValueField(object)).byteValue();
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static ProtoToRowDataConverter createSmallIntConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.INT32 || schemaType == Type.SINT32 || schemaType == Type.SFIXED32) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) object).shortValue();
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) extractValueField(object)).shortValue();
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static ProtoToRowDataConverter createIntegerConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.INT32 || schemaType == Type.SINT32 || schemaType == Type.SFIXED32) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) object).intValue();
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) extractValueField(object)).intValue();
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static ProtoToRowDataConverter createDoubleConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.DOUBLE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) object).doubleValue();
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) extractValueField(object)).doubleValue();
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static ProtoToRowDataConverter createFloatConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.FLOAT) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) object).floatValue();
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) extractValueField(object)).floatValue();
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static ProtoToRowDataConverter createBigintConverter(
            LogicalType targetType, Type schemaType) {
        if (schemaType == Type.UINT32
                || schemaType == Type.FIXED32
                || schemaType == Type.INT64
                || schemaType == Type.UINT64
                || schemaType == Type.SINT64
                || schemaType == Type.FIXED64
                || schemaType == Type.SFIXED64) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) object).longValue();
                }
            };
        } else if (schemaType == Type.MESSAGE) {
            return new ProtoToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return ((Number) extractValueField(object)).byteValue();
                }
            };
        } else {
            throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
        }
    }

    private static IllegalStateException unexpectedTypeForSchema(
            Type schemaType, LogicalTypeRoot flinkType) {
        return new IllegalStateException(
                String.format(
                        "Unsupported protobuf type: %s for a SQL type: %s", schemaType, flinkType));
    }

    private static ProtoToRowDataConverter createDecimalConverter(DecimalType targetType) {
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                final ByteString valueField = (ByteString) extractValueField(object);
                return DecimalData.fromUnscaledBytes(
                        valueField.toByteArray(), targetType.getPrecision(), targetType.getScale());
            }
        };
    }

    private static ProtoToRowDataConverter createDateConverter() {
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                Message message = (Message) object;
                int year = 0;
                int month = 0;
                int day = 0;
                for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
                    final String fieldName = entry.getKey().getName();
                    if (fieldName.equals("year")) {
                        year = ((Number) entry.getValue()).intValue();
                    } else if (fieldName.equals("month")) {
                        month = ((Number) entry.getValue()).intValue();
                    } else if (fieldName.equals("day")) {
                        day = ((Number) entry.getValue()).intValue();
                    }
                }
                return DateTimeUtils.toInternal(LocalDate.of(year, month, day));
            }
        };
    }

    private static ProtoToRowDataConverter createTimestampConverter() {
        return new ProtoToRowDataConverter() {
            @Override
            public Object convert(Object object) throws IOException {
                final Message message = (Message) object;
                long seconds = 0L;
                int nanos = 0;
                for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
                    final String fieldName = entry.getKey().getName();
                    if (fieldName.equals("seconds")) {
                        seconds = ((Number) entry.getValue()).longValue();
                    } else if (fieldName.equals("nanos")) {
                        nanos = ((Number) entry.getValue()).intValue();
                    }
                }
                long millis = Math.addExact(Math.multiplyExact(seconds, 1000L), nanos / 1000_000L);
                int nanosOfMillis = nanos % 1000_000;

                return TimestampData.fromEpochMillis(millis, nanosOfMillis);
            }
        };
    }

    private static Object extractValueField(Object value) {
        final Message message = (Message) value;
        final FieldDescriptor fieldDescriptor =
                message.getDescriptorForType().findFieldByName("value");

        return message.getField(fieldDescriptor);
    }
}
