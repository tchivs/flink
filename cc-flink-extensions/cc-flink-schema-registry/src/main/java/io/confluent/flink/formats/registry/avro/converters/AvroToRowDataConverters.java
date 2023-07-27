/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.flink.formats.registry.avro.converters;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. * */
@Internal
public class AvroToRowDataConverters {

    private static final Schema NULL_AVRO_SCHEMA = Schema.create(Type.NULL);

    /**
     * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface AvroToRowDataConverter extends Serializable {

        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Avro for
    // sql-client uber jars.
    // --------------------------------------------------------------------------------

    /** Creates a runtime converter. */
    public static AvroToRowDataConverter createConverter(
            Schema readSchema, LogicalType targetType) {
        switch (readSchema.getType()) {
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return new AvroToRowDataConverter() {
                    @Override
                    public Object convert(Object object) {
                        return object;
                    }
                };
            case BYTES:
                return createDecimalConverter(
                        (DecimalType) targetType,
                        new Function<Object, byte[]>() {
                            @Override
                            public byte[] apply(Object o) {
                                ByteBuffer byteBuffer = (ByteBuffer) o;
                                byte[] bytes = new byte[byteBuffer.remaining()];
                                byteBuffer.get(bytes);
                                return bytes;
                            }
                        });
            case FIXED:
                return createDecimalConverter(
                        (DecimalType) targetType,
                        new Function<Object, byte[]>() {
                            @Override
                            public byte[] apply(Object o) {
                                return ((GenericFixed) o).bytes();
                            }
                        });
            case INT:
                return createIntegerConverter(targetType);
            case LONG:
                return createLongConverter(targetType);
            case ENUM:
            case STRING:
                return new AvroToRowDataConverter() {
                    @Override
                    public Object convert(Object object) {
                        return StringData.fromString(object.toString());
                    }
                };
            case ARRAY:
                return createArrayConverter(readSchema, targetType);

            case MAP:
                if (targetType.is(LogicalTypeRoot.MAP)) {
                    return createMapConverter(readSchema, (MapType) targetType);
                } else if (targetType.is(LogicalTypeRoot.MULTISET)) {
                    return createMapToMultiSetConverter();
                } else {
                    throw new IllegalStateException("Unexpected target type: " + targetType);
                }

            case RECORD:
                return createRecordConverter(readSchema, (RowType) targetType);

            case NULL:
                return new AvroToRowDataConverter() {
                    @Override
                    public Object convert(Object object) {
                        return null;
                    }
                };
            case UNION:
                return createUnionConverter(readSchema, targetType);
            default:
                throw new IllegalStateException(
                        "Couldn't translate unsupported schema type "
                                + readSchema.getType().getName()
                                + ".");
        }
    }

    private static AvroToRowDataConverter createUnionConverter(
            Schema readSchema, LogicalType targetType) {
        if (readSchema.getTypes().size() == 2) {
            if (readSchema.getTypes().contains(NULL_AVRO_SCHEMA)) {
                for (Schema memberSchema : readSchema.getTypes()) {
                    if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
                        final AvroToRowDataConverter nestedConverter =
                                createConverter(memberSchema, targetType);
                        return new AvroToRowDataConverter() {
                            @Override
                            public Object convert(Object object) {
                                if (object == null) {
                                    return null;
                                }

                                return nestedConverter.convert(object);
                            }
                        };
                    }
                }
            }
        }

        // TODO: add runtime support for reading UNIONs first
        throw new IllegalArgumentException("Custom UNION types are not yet supported.");
    }

    private static AvroToRowDataConverter createRecordConverter(
            Schema readSchema, RowType targetType) {
        final List<Field> avroFields = readSchema.getFields();
        final AvroToRowDataConverter[] fieldConverters =
                IntStream.range(0, targetType.getFieldCount())
                        .mapToObj(
                                idx ->
                                        createConverter(
                                                avroFields.get(idx).schema(),
                                                targetType.getTypeAt(idx)))
                        .toArray(AvroToRowDataConverter[]::new);
        final int arity = targetType.getFieldCount();

        return new AvroToRowDataConverter() {
            @Override
            public Object convert(Object object) {
                IndexedRecord record = (IndexedRecord) object;
                GenericRowData row = new GenericRowData(arity);
                for (int i = 0; i < arity; ++i) {
                    row.setField(i, fieldConverters[i].convert(record.get(i)));
                }
                return row;
            }
        };
    }

    private static AvroToRowDataConverter createMapConverter(
            Schema readSchema, MapType targetType) {
        final AvroToRowDataConverter valueConverter =
                createConverter(readSchema.getValueType(), targetType.getValueType());

        return new AvroToRowDataConverter() {
            @Override
            public Object convert(Object object) {
                final Map<?, ?> map = (Map<?, ?>) object;
                Map<Object, Object> result = new HashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Object key = StringData.fromString(entry.getKey().toString());
                    Object value = valueConverter.convert(entry.getValue());
                    result.put(key, value);
                }
                return new GenericMapData(result);
            }
        };
    }

    private static AvroToRowDataConverter createMapToMultiSetConverter() {
        return new AvroToRowDataConverter() {
            @Override
            public Object convert(Object object) {
                final Map<?, ?> map = (Map<?, ?>) object;
                Map<Object, Object> result = new HashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Object key = StringData.fromString(entry.getKey().toString());
                    // this must be an integer
                    Object value = entry.getValue();
                    result.put(key, value);
                }
                return new GenericMapData(result);
            }
        };
    }

    private static AvroToRowDataConverter createArrayConverter(
            Schema readSchema, LogicalType targetType) {
        if (targetType.is(LogicalTypeRoot.ARRAY)) {
            return createArrayToArrayConverter(readSchema, (ArrayType) targetType);
        } else if (targetType.is(LogicalTypeRoot.MAP)) {
            return createArrayToMapConverter(readSchema, (MapType) targetType);
        } else if (targetType.is(LogicalTypeRoot.MULTISET)) {
            return createArrayToMultisetConverter(readSchema, (MultisetType) targetType);
        } else {
            throw new IllegalStateException("Unexpected target type: " + targetType);
        }
    }

    private static AvroToRowDataConverter createLongConverter(LogicalType targetType) {
        switch (targetType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = ((TimestampType) targetType).getPrecision();
                    return createTimestampConverter(precision);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = ((LocalZonedTimestampType) targetType).getPrecision();
                    return createTimestampConverter(precision);
                }
            case TIME_WITHOUT_TIME_ZONE:
                throw new IllegalStateException(
                        "Precision > 3 is not supported in Flink runtime yet.");
            case BIGINT:
                return new AvroToRowDataConverter() {
                    @Override
                    public Object convert(Object object) {
                        return object;
                    }
                };
            default:
                throw new IllegalStateException("Unexpected target type: " + targetType);
        }
    }

    private static AvroToRowDataConverter createTimestampConverter(int precision) {
        if (precision <= 3) {
            return new AvroToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    return TimestampData.fromEpochMillis((Long) object);
                }
            };
        } else if (precision <= 6) {
            return new AvroToRowDataConverter() {
                @Override
                public Object convert(Object object) {
                    final Long micros = (Long) object;
                    final long millis = micros / 1000;
                    final long nanosOfMilli = micros % 1_000 * 1_000;
                    return TimestampData.fromEpochMillis(millis, (int) nanosOfMilli);
                }
            };
        } else {
            throw new IllegalStateException("Unsupported precision: " + precision);
        }
    }

    private static AvroToRowDataConverter createIntegerConverter(LogicalType targetType) {
        switch (targetType.getTypeRoot()) {
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new AvroToRowDataConverter() {
                    @Override
                    public Object convert(Object object) {
                        return object;
                    }
                };
            case TINYINT:
                return new AvroToRowDataConverter() {
                    @Override
                    public Object convert(Object object) {
                        return ((Integer) object).byteValue();
                    }
                };
            case SMALLINT:
                return new AvroToRowDataConverter() {
                    @Override
                    public Object convert(Object object) {
                        return ((Integer) object).shortValue();
                    }
                };
            default:
                throw new IllegalStateException(
                        "Unexpected target Flink type: " + targetType.asSummaryString());
        }
    }

    private static AvroToRowDataConverter createDecimalConverter(
            DecimalType decimalType, Function<Object, byte[]> toUnscaledBytes) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return new AvroToRowDataConverter() {
            @Override
            public Object convert(Object object) {
                return DecimalData.fromUnscaledBytes(
                        toUnscaledBytes.apply(object), precision, scale);
            }
        };
    }

    private static AvroToRowDataConverter createArrayToArrayConverter(
            Schema readSchema, ArrayType arrayType) {
        final AvroToRowDataConverter elementConverter =
                createConverter(readSchema.getElementType(), arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

        return new AvroToRowDataConverter() {
            @Override
            public Object convert(Object object) {
                final List<?> list = (List<?>) object;
                final int length = list.size();
                final Object[] array = (Object[]) Array.newInstance(elementClass, length);
                for (int i = 0; i < length; ++i) {
                    array[i] = elementConverter.convert(list.get(i));
                }
                return new GenericArrayData(array);
            }
        };
    }

    private static AvroToRowDataConverter createArrayToMapConverter(
            Schema readSchema, MapType targetType) {
        final Schema keySchema = readSchema.getElementType().getField("key").schema();
        final Schema valueSchema = readSchema.getElementType().getField("value").schema();
        final AvroToRowDataConverter keyConverter =
                createConverter(keySchema, targetType.getKeyType());
        final AvroToRowDataConverter valueConverter =
                createConverter(valueSchema, targetType.getValueType());

        return new AvroToRowDataConverter() {
            @Override
            public Object convert(Object object) {
                final List<?> list = (List<?>) object;
                final int length = list.size();
                final Map<Object, Object> map = new HashMap<>();
                for (int i = 0; i < length; ++i) {
                    final GenericRecord mapEntry = (GenericRecord) list.get(i);
                    Object key = keyConverter.convert(mapEntry.get("key"));
                    Object value = valueConverter.convert(mapEntry.get("value"));
                    map.put(key, value);
                }
                return new GenericMapData(map);
            }
        };
    }

    private static AvroToRowDataConverter createArrayToMultisetConverter(
            Schema readSchema, MultisetType targetType) {
        final Schema keySchema = readSchema.getElementType().getField("key").schema();
        final AvroToRowDataConverter keyConverter =
                createConverter(keySchema, targetType.getElementType());

        return new AvroToRowDataConverter() {
            @Override
            public Object convert(Object object) {
                final List<?> list = (List<?>) object;
                final int length = list.size();
                final Map<Object, Object> map = new HashMap<>();
                for (int i = 0; i < length; ++i) {
                    final GenericRecord mapEntry = (GenericRecord) list.get(i);
                    Object key = keyConverter.convert(mapEntry.get("key"));
                    // this must be an int
                    Object value = mapEntry.get("value");
                    map.put(key, value);
                }
                return new GenericMapData(map);
            }
        };
    }
}
