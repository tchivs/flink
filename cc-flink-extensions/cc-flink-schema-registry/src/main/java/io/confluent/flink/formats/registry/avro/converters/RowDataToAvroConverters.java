/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.avro.converters;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ArrayData.ElementGetter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Tool class used to convert from {@link RowData} to Avro {@link GenericRecord}. */
@Confluent
public class RowDataToAvroConverters {

    // --------------------------------------------------------------------------------
    // Runtime Converters
    // --------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding Avro data structures.
     */
    @FunctionalInterface
    public interface RowDataToAvroConverter extends Serializable {

        Object convert(Object object);
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Avro for
    // sql-client uber jars.
    // --------------------------------------------------------------------------------

    /**
     * Creates a runtime converter according to the given logical type that converts objects of
     * Flink Table & SQL internal data structures to corresponding Avro data structures.
     */
    public static RowDataToAvroConverter createConverter(LogicalType type, Schema targetSchema) {

        if (type.isNullable()) {
            final Optional<Schema> nonNullType = extractTypeFromNullableUnion(targetSchema);
            if (!nonNullType.isPresent()) {
                throw new IllegalArgumentException(
                        "Nullable type should map to avro union. Got: " + targetSchema);
            }

            final RowDataToAvroConverter converter =
                    createConverterNotNull(type, nonNullType.get());

            // wrap into nullable converter
            return new RowDataToAvroConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object convert(Object object) {
                    if (object == null) {
                        return null;
                    }

                    return converter.convert(object);
                }
            };
        } else {
            return createConverterNotNull(type, targetSchema);
        }
    }

    private static Optional<Schema> extractTypeFromNullableUnion(Schema targetSchema) {
        if (!targetSchema.isUnion()) {
            return Optional.empty();
        }
        final List<Schema> nonNullUnionTypes =
                targetSchema.getTypes().stream()
                        .filter(t -> t.getType() != Type.NULL)
                        .collect(Collectors.toList());

        if (nonNullUnionTypes.size() == 1) {
            return Optional.of(nonNullUnionTypes.get(0));
        } else {
            return Optional.of(Schema.createUnion(nonNullUnionTypes));
        }
    }

    private static RowDataToAvroConverter createConverterNotNull(
            LogicalType type, Schema targetSchema) {
        final RowDataToAvroConverter converter;
        switch (type.getTypeRoot()) {
            case NULL:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Object object) {
                                return null;
                            }
                        };
                break;
            case TINYINT:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Object object) {
                                return ((Byte) object).intValue();
                            }
                        };
                break;
            case SMALLINT:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Object object) {
                                return ((Short) object).intValue();
                            }
                        };
                break;
            case BOOLEAN: // boolean
            case INTEGER: // int
            case INTERVAL_YEAR_MONTH: // long
            case BIGINT: // long
            case INTERVAL_DAY_TIME: // long
            case FLOAT: // float
            case DOUBLE: // double
            case TIME_WITHOUT_TIME_ZONE: // int
            case DATE: // int
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Object object) {
                                return object;
                            }
                        };
                break;
            case CHAR:
            case VARCHAR:
                converter = createStringConverter(targetSchema);
                break;
            case BINARY:
            case VARBINARY:
                converter = createBinaryConverter(targetSchema);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) type;
                converter = createTimestampConverter(localZonedTimestampType.getPrecision());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final TimestampType timestampType = (TimestampType) type;
                converter = createTimestampConverter(timestampType.getPrecision());
                break;
            case DECIMAL:
                converter = createDecimalConverter(targetSchema);
                break;
            case ARRAY:
                converter = createArrayConverter((ArrayType) type, targetSchema);
                break;
            case ROW:
                converter = createRowConverter((RowType) type, targetSchema);
                break;
            case MAP:
                converter = createMapConverter((MapType) type, targetSchema);
                break;
            case MULTISET:
                converter = createMultisetConverter((MultisetType) type, targetSchema);
                break;
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        return converter;
    }

    private static RowDataToAvroConverter createDecimalConverter(Schema targetSchema) {

        switch (targetSchema.getType()) {
            case FIXED:
                return new RowDataToAvroConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object object) {
                        final byte[] unscaledBytes = ((DecimalData) object).toUnscaledBytes();
                        return new GenericData.Fixed(targetSchema, unscaledBytes);
                    }
                };
            case BYTES:
                return new RowDataToAvroConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object object) {
                        return ByteBuffer.wrap(((DecimalData) object).toUnscaledBytes());
                    }
                };
            default:
                throw new IllegalStateException(
                        "Unsupported target type for a DECIMAL: " + targetSchema);
        }
    }

    private static RowDataToAvroConverter createTimestampConverter(int precision) {
        if (precision <= 3) {
            return new RowDataToAvroConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object convert(Object object) {
                    return ((TimestampData) object).toInstant().toEpochMilli();
                }
            };
        } else if (precision <= 6) {
            return new RowDataToAvroConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object convert(Object object) {
                    // copied over from
                    // org.apache.avro.data.TimeConversions.TimestampMicrosConversion.toLong
                    final Instant instant = ((TimestampData) object).toInstant();
                    long seconds = instant.getEpochSecond();
                    int nanos = instant.getNano();

                    if (seconds < 0 && nanos > 0) {
                        long micros = Math.multiplyExact(seconds + 1, 1_000_000L);
                        long adjustment = (nanos / 1_000L) - 1_000_000;

                        return Math.addExact(micros, adjustment);
                    } else {
                        long micros = Math.multiplyExact(seconds, 1_000_000L);

                        return Math.addExact(micros, nanos / 1_000L);
                    }
                }
            };
        } else {
            throw new IllegalStateException("Unsupported timestamp precision: " + precision);
        }
    }

    private static RowDataToAvroConverter createBinaryConverter(Schema targetSchema) {
        switch (targetSchema.getType()) {
            case FIXED:
                return new RowDataToAvroConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object object) {
                        return new GenericData.Fixed(targetSchema, (byte[]) object);
                    }
                };
            case BYTES:
                return new RowDataToAvroConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object object) {
                        return ByteBuffer.wrap((byte[]) object);
                    }
                };
            default:
                throw new IllegalStateException(
                        "Unsupported target type for a BINARY: " + targetSchema);
        }
    }

    private static RowDataToAvroConverter createStringConverter(Schema targetSchema) {
        switch (targetSchema.getType()) {
            case STRING:
                return new RowDataToAvroConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object object) {
                        return new Utf8(object.toString());
                    }
                };
            case ENUM:
                return new RowDataToAvroConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object object) {
                        return new GenericData.EnumSymbol(targetSchema, object.toString());
                    }
                };
            default:
                throw new IllegalStateException(
                        "Unsupported target type for a STRING: " + targetSchema);
        }
    }

    private static RowDataToAvroConverter createRowConverter(RowType rowType, Schema targetSchema) {
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }
        final int length = rowType.getFieldCount();

        switch (targetSchema.getType()) {
            case RECORD:
                {
                    final List<Field> avroFields = targetSchema.getFields();
                    final RowDataToAvroConverter[] fieldConverters =
                            IntStream.range(0, rowType.getFieldCount())
                                    .mapToObj(
                                            idx ->
                                                    RowDataToAvroConverters.createConverter(
                                                            rowType.getTypeAt(idx),
                                                            avroFields.get(idx).schema()))
                                    .toArray(RowDataToAvroConverter[]::new);
                    return createRowRecordConverter(
                            targetSchema, length, fieldConverters, fieldGetters);
                }
            case UNION:
                {
                    final List<Schema> avroUnionSubtypes = targetSchema.getTypes();
                    final RowDataToAvroConverter[] fieldConverters =
                            IntStream.range(0, rowType.getFieldCount())
                                    .mapToObj(
                                            idx ->
                                                    RowDataToAvroConverters.createConverterNotNull(
                                                            rowType.getTypeAt(idx),
                                                            avroUnionSubtypes.get(idx)))
                                    .toArray(RowDataToAvroConverter[]::new);
                    return createRowUnionConverter(
                            targetSchema, length, fieldGetters, fieldConverters);
                }
            default:
                throw new IllegalStateException(
                        "Unsupported target type for a ROW: " + targetSchema);
        }
    }

    private static RowDataToAvroConverter createRowUnionConverter(
            Schema targetSchema,
            int length,
            FieldGetter[] fieldGetters,
            RowDataToAvroConverter[] fieldConverters) {
        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                final RowData row = (RowData) object;
                for (int i = 0; i < length; ++i) {
                    try {

                        final Object fieldOrNull = fieldGetters[i].getFieldOrNull(row);
                        if (fieldOrNull != null) {
                            return fieldConverters[i].convert(fieldOrNull);
                        }
                    } catch (Throwable t) {
                        throw new RuntimeException(
                                String.format("Fail to serialize union: %s.", targetSchema), t);
                    }
                }
                return null;
            }
        };
    }

    private static RowDataToAvroConverter createRowRecordConverter(
            Schema targetSchema,
            int length,
            RowDataToAvroConverter[] fieldConverters,
            FieldGetter[] fieldGetters) {
        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                final RowData row = (RowData) object;
                final GenericRecord record = new GenericData.Record(targetSchema);
                for (int i = 0; i < length; ++i) {
                    try {
                        Object avroObject =
                                fieldConverters[i].convert(fieldGetters[i].getFieldOrNull(row));
                        record.put(i, avroObject);
                    } catch (Throwable t) {
                        throw new RuntimeException(
                                String.format(
                                        "Fail to serialize at field: %s.",
                                        targetSchema.getFields().get(i).name()),
                                t);
                    }
                }
                return record;
            }
        };
    }

    private static RowDataToAvroConverter createArrayConverter(
            ArrayType arrayType, Schema targetSchema) {
        LogicalType elementType = arrayType.getElementType();
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        final RowDataToAvroConverter elementConverter =
                createConverter(arrayType.getElementType(), targetSchema.getElementType());

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                ArrayData arrayData = (ArrayData) object;
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < arrayData.size(); ++i) {
                    list.add(
                            elementConverter.convert(elementGetter.getElementOrNull(arrayData, i)));
                }
                return list;
            }
        };
    }

    private static RowDataToAvroConverter createMultisetConverter(
            MultisetType type, Schema targetSchema) {
        final LogicalType keyType = type.getElementType();
        final LogicalType valueType = new IntType(false);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        final ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);

        return createMapConverter(targetSchema, keyType, valueType, valueGetter, keyGetter);
    }

    private static RowDataToAvroConverter createMapConverter(MapType type, Schema targetSchema) {
        final LogicalType keyType = type.getKeyType();
        final LogicalType valueType = type.getValueType();
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        final ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);

        return createMapConverter(targetSchema, keyType, valueType, valueGetter, keyGetter);
    }

    private static RowDataToAvroConverter createMapConverter(
            Schema targetSchema,
            LogicalType keyType,
            LogicalType valueType,
            ElementGetter valueGetter,
            ElementGetter keyGetter) {
        switch (targetSchema.getType()) {
            case MAP:
                {
                    final RowDataToAvroConverter valueConverter =
                            createConverter(valueType, targetSchema.getValueType());
                    return new RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Object object) {
                            final MapData mapData = (MapData) object;
                            final ArrayData keyArray = mapData.keyArray();
                            final ArrayData valueArray = mapData.valueArray();
                            final Map<Object, Object> map = new HashMap<>(mapData.size());
                            for (int i = 0; i < mapData.size(); ++i) {
                                final String key = keyArray.getString(i).toString();
                                final Object value =
                                        valueConverter.convert(
                                                valueGetter.getElementOrNull(valueArray, i));
                                map.put(key, value);
                            }
                            return map;
                        }
                    };
                }
            case ARRAY:
                {
                    final Schema elementType = targetSchema.getElementType();
                    final RowDataToAvroConverter keyConverter =
                            createConverter(keyType, elementType.getField("key").schema());
                    final RowDataToAvroConverter valueConverter =
                            createConverter(valueType, elementType.getField("value").schema());
                    return new RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Object object) {
                            final MapData mapData = (MapData) object;
                            final ArrayData keyArray = mapData.keyArray();
                            final ArrayData valueArray = mapData.valueArray();
                            List<Object> list = new ArrayList<>();
                            for (int i = 0; i < mapData.size(); ++i) {
                                final Object key =
                                        keyConverter.convert(
                                                keyGetter.getElementOrNull(keyArray, i));
                                final Object value =
                                        valueConverter.convert(
                                                valueGetter.getElementOrNull(valueArray, i));
                                list.add(
                                        new GenericRecordBuilder(elementType)
                                                .set("key", key)
                                                .set("value", value)
                                                .build());
                            }
                            return list;
                        }
                    };
                }
            default:
                throw new IllegalStateException(
                        "Unsupported target type for a MAP: " + targetSchema);
        }
    }
}
