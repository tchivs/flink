/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

/** This class encapsulates the json serialization of input data for remote model calls. */
public class DataSerializer {

    /**
     * This interface is used to serialize input data for remote model calls. The input data is
     * serialized into a JsonNode.
     */
    public interface InputSerializer {
        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);

        default String toString(Object value) {
            return value.toString();
        }

        /** Convert to a list of little-endian byte array. */
        default List<byte[]> toBytes(Object value) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    /**
     * This interface is used to deserialize output data from remote model calls. The output data is
     * deserialized from a JsonNode.
     */
    public interface OutputDeserializer {
        Object convert(JsonNode object) throws IOException;

        // * Convert a flat array to a possibly multidimensional array. */
        default Object convert(JsonNode object, int offset, int length, List<Integer> shape)
                throws IOException {
            // The shape is only used for array types or things that contain arrays.
            return convert(object.get(offset));
        }

        Object convert(String object) throws IOException;

        Object convert(
                ByteBuffer buffer, int length, List<Integer> shape, ByteReinterpreter typeOverride)
                throws IOException;

        default int bytesConsumed() {
            return 0;
        }

        default Object convert(List<String> list) throws IOException {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    /** This interface is used to reinterpret bytes as a different type. */
    public interface ByteReinterpreter {
        default long reinterpretLong(ByteBuffer buffer, int length, List<Integer> shape) {
            return reinterpretInt(buffer, length, shape);
        }

        default double reinterpretDouble(ByteBuffer buffer, int length, List<Integer> shape) {
            return reinterpretFloat(buffer, length, shape);
        }

        default float reinterpretFloat(ByteBuffer buffer, int length, List<Integer> shape) {
            throw new UnsupportedOperationException("Not implemented");
        }

        default int reinterpretInt(ByteBuffer buffer, int length, List<Integer> shape) {
            return reinterpretShort(buffer, length, shape);
        }

        default short reinterpretShort(ByteBuffer buffer, int length, List<Integer> shape) {
            throw new UnsupportedOperationException("Not implemented");
        }

        default byte[] reinterpretBytes(ByteBuffer buffer, int length, List<Integer> shape) {
            throw new UnsupportedOperationException("Not implemented");
        }

        int bytesConsumed();
    }

    public static InputSerializer getSerializer(LogicalType inputType) {
        return createSerializer(inputType);
    }

    public static OutputDeserializer getDeserializer(LogicalType outputType) {
        // Create a schema from the input type.
        return createDeserializer(outputType);
    }

    public static InputSerializer createSerializer(LogicalType type) {

        if (type.isNullable()) {
            final InputSerializer serializer = createSerializerNotNull(type);

            // wrap into nullable Serializer
            return new InputSerializer() {
                @Override
                public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                    if (value == null) {
                        return mapper.nullNode();
                    }

                    return serializer.convert(mapper, reuse, value);
                }

                @Override
                public String toString(Object value) {
                    if (value == null) {
                        return "";
                    }

                    return serializer.toString(value);
                }

                @Override
                public List<byte[]> toBytes(Object value) {
                    if (value == null) {
                        return Collections.emptyList();
                    }

                    return serializer.toBytes(value);
                }
            };
        } else {
            return createSerializerNotNull(type);
        }
    }

    public static InputSerializer createSerializerNotNull(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.nullNode();
                    }

                    @Override
                    public String toString(Object value) {
                        return "";
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        return Collections.emptyList();
                    }
                };
            case TINYINT:
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        // Cast to short to create a ShortNode, otherwise an IntNode will be created
                        return mapper.getNodeFactory().numberNode((short) (byte) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        return Collections.singletonList(new byte[] {(byte) value});
                    }
                };
            case SMALLINT:
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((short) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        return Collections.singletonList(
                                new byte[] {(byte) (short) value, (byte) ((short) value >> 8)});
                    }
                };
            case BOOLEAN: // boolean
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().booleanNode((boolean) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        return Collections.singletonList(
                                new byte[] {(byte) ((boolean) value ? 1 : 0)});
                    }
                };
            case INTEGER: // int
            case TIME_WITHOUT_TIME_ZONE: // int
            case DATE: // int
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((int) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        return Collections.singletonList(
                                new byte[] {
                                    (byte) (int) value,
                                    (byte) ((int) value >> 8),
                                    (byte) ((int) value >> 16),
                                    (byte) ((int) value >> 24)
                                });
                    }
                };
            case BIGINT: // long
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((long) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        return Collections.singletonList(
                                new byte[] {
                                    (byte) (long) value,
                                    (byte) ((long) value >> 8),
                                    (byte) ((long) value >> 16),
                                    (byte) ((long) value >> 24),
                                    (byte) ((long) value >> 32),
                                    (byte) ((long) value >> 40),
                                    (byte) ((long) value >> 48),
                                    (byte) ((long) value >> 56)
                                });
                    }
                };
            case FLOAT: // float
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((float) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        int intBits = Float.floatToRawIntBits((float) value);
                        return Collections.singletonList(
                                new byte[] {
                                    (byte) intBits,
                                    (byte) (intBits >> 8),
                                    (byte) (intBits >> 16),
                                    (byte) (intBits >> 24)
                                });
                    }
                };
            case DOUBLE: // double
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((double) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        long longBits = Double.doubleToRawLongBits((double) value);
                        return Collections.singletonList(
                                new byte[] {
                                    (byte) longBits,
                                    (byte) (longBits >> 8),
                                    (byte) (longBits >> 16),
                                    (byte) (longBits >> 24),
                                    (byte) (longBits >> 32),
                                    (byte) (longBits >> 40),
                                    (byte) (longBits >> 48),
                                    (byte) (longBits >> 56)
                                });
                    }
                };
            case CHAR:
            case VARCHAR:
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().textNode(value.toString());
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        return Collections.singletonList(
                                value.toString().getBytes(StandardCharsets.UTF_8));
                    }
                };
            case BINARY:
            case VARBINARY:
                return new InputSerializer() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().binaryNode((byte[]) value);
                    }

                    @Override
                    public String toString(Object value) {
                        return java.util.Base64.getEncoder().encodeToString((byte[]) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        return Collections.singletonList((byte[]) value);
                    }
                };
            case DECIMAL:
                return new InputSerializer() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((BigDecimal) value);
                    }

                    @Override
                    public List<byte[]> toBytes(Object value) {
                        // Neither TensorFlow nor ONNX support Decimal types, so I don't know
                        // who is going to use this. But here it is, in little-endian order like
                        // all the other types.
                        final BigDecimal decimalData = (BigDecimal) value;
                        byte[] bigEndian = decimalData.unscaledValue().toByteArray();
                        byte[] littleEndian = new byte[bigEndian.length];
                        for (int i = 0; i < bigEndian.length; i++) {
                            littleEndian[i] = bigEndian[bigEndian.length - 1 - i];
                        }
                        return Collections.singletonList(littleEndian);
                    }
                };
            case ARRAY:
                return createArraySerializer((ArrayType) type);
            case ROW:
                return createRowSerializer((RowType) type);
            case MAP:
            case MULTISET:
            case RAW:
            case INTERVAL_YEAR_MONTH: // long
            case INTERVAL_DAY_TIME: // long
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type for model input: " + type);
        }
    }

    @FunctionalInterface
    interface ElementGetter {
        Object get(int i);
    }

    private static InputSerializer createArraySerializer(ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        final InputSerializer elementSerializer = createSerializer(elementType);

        return new InputSerializer() {
            private int getLength(Object array) {
                // The inputs here are generally expected to be Object[], but we also support
                // List and primitive arrays in case non-default external conversions are used.
                if (array instanceof Object[]) {
                    return ((Object[]) array).length;
                } else if (array instanceof List) {
                    return ((List) array).size();
                } else {
                    return Array.getLength(array);
                }
            }

            private ElementGetter getElement(Object array) {
                if (array instanceof Object[]) {
                    Object[] objectArray = (Object[]) array;
                    return i -> objectArray[i];
                } else if (array instanceof List) {
                    List<?> list = (List<?>) array;
                    return i -> list.get(i);
                } else {
                    return i -> Array.get(array, i);
                }
            }

            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ArrayNode node;

                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createArrayNode();
                } else {
                    node = (ArrayNode) reuse;
                    node.removeAll();
                }

                int numElements = getLength(value);
                ElementGetter array = getElement(value);
                for (int i = 0; i < numElements; i++) {
                    Object element = array.get(i);
                    node.add(elementSerializer.convert(mapper, null, element));
                }

                return node;
            }

            @Override
            public String toString(Object value) {
                int numElements = getLength(value);
                ElementGetter array = getElement(value);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < numElements; i++) {
                    Object element = array.get(i);
                    sb.append(elementSerializer.toString(element));
                    if (i < numElements - 1) {
                        sb.append(",");
                    }
                }
                return sb.toString();
            }

            @Override
            public List<byte[]> toBytes(Object value) {
                ElementGetter array = getElement(value);
                int numElements = getLength(value);
                List<byte[]> bytes = new ArrayList<>(numElements);
                for (int i = 0; i < numElements; i++) {
                    bytes.addAll(elementSerializer.toBytes(array.get(i)));
                }
                return bytes;
            }
        };
    }

    private static InputSerializer createRowSerializer(RowType rowType) {
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        final InputSerializer[] fieldSerializers =
                rowType.getFields().stream()
                        .map(field -> createSerializer(field.getType()))
                        .toArray(InputSerializer[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final int length = rowType.getFieldCount();

        return new InputSerializer() {
            private Object getFieldValue(Row row, String name, int position) {
                if (row.getFieldNames(false) == null) {
                    return row.getField(position);
                } else {
                    return row.getField(name);
                }
            }

            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ObjectNode node;
                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createObjectNode();
                } else {
                    node = (ObjectNode) reuse;
                }
                final Row row = (Row) value;
                for (int i = 0; i < length; ++i) {
                    String fieldName = fieldNames[i];
                    try {
                        Object field = getFieldValue(row, fieldName, i);
                        if (field != null) {
                            node.set(
                                    fieldName,
                                    fieldSerializers[i].convert(
                                            mapper, node.get(fieldName), field));
                        }
                    } catch (Throwable t) {
                        throw new FlinkRuntimeException(
                                String.format("Fail to serialize at field: %s.", fieldName), t);
                    }
                }
                return node;
            }

            @Override
            public String toString(Object value) {
                final Row row = (Row) value;
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < length; ++i) {
                    try {
                        Object field = getFieldValue(row, fieldNames[i], i);
                        sb.append(fieldSerializers[i].toString(field));
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    } catch (Throwable t) {
                        throw new FlinkRuntimeException(
                                String.format("Fail to serialize at field: %s.", fieldNames[i]), t);
                    }
                }
                return sb.toString();
            }

            @Override
            public List<byte[]> toBytes(Object value) {
                final Row row = (Row) value;
                List<byte[]> bytes = new ArrayList<>();
                for (int i = 0; i < length; ++i) {
                    Object field = getFieldValue(row, fieldNames[i], i);
                    bytes.addAll(fieldSerializers[i].toBytes(field));
                }
                return bytes;
            }
        };
    }

    public static OutputDeserializer createDeserializer(LogicalType targetType) {
        if (targetType.isNullable()) {
            final OutputDeserializer nonNullConverter = createNonNullDeserializer(targetType);
            return new OutputDeserializer() {
                @Override
                public Object convert(JsonNode object) throws IOException {
                    if (object.isNull()) {
                        return null;
                    } else {
                        return nonNullConverter.convert(object);
                    }
                }

                @Override
                public Object convert(JsonNode object, int offset, int length, List<Integer> shape)
                        throws IOException {
                    if (object.isNull()) {
                        return null;
                    } else {
                        return nonNullConverter.convert(object, offset, length, shape);
                    }
                }

                @Override
                public Object convert(String object) throws IOException {
                    if (object == null) {
                        return null;
                    } else {
                        return nonNullConverter.convert(object);
                    }
                }

                @Override
                public Object convert(
                        ByteBuffer buffer,
                        int length,
                        List<Integer> shape,
                        ByteReinterpreter typeOverride)
                        throws IOException {
                    if (buffer == null || !buffer.hasRemaining()) {
                        return null;
                    } else {
                        return nonNullConverter.convert(buffer, length, shape, typeOverride);
                    }
                }

                @Override
                public Object convert(List<String> list) throws IOException {
                    if (list == null) {
                        return null;
                    } else {
                        return nonNullConverter.convert(list);
                    }
                }

                @Override
                public int bytesConsumed() {
                    return nonNullConverter.bytesConsumed();
                }
            };
        } else {
            return createNonNullDeserializer(targetType);
        }
    }

    private static OutputDeserializer createNonNullDeserializer(LogicalType targetType) {
        switch (targetType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) {
                        return object.textValue();
                    }

                    @Override
                    public Object convert(String object) {
                        return object;
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride) {
                        if (typeOverride != null) {
                            byte[] bytes = typeOverride.reinterpretBytes(buffer, length, shape);
                            return new String(bytes, StandardCharsets.UTF_8);
                        }
                        // If shape is set here, it should be [length], but we don't verify.
                        if (buffer.hasArray()) {
                            String s =
                                    new String(
                                            buffer.array(),
                                            buffer.arrayOffset() + buffer.position(),
                                            length,
                                            StandardCharsets.UTF_8);
                            buffer.position(buffer.position() + length);
                            return s;
                        } else {
                            byte[] bytes = new byte[length];
                            buffer.get(bytes);
                            return new String(bytes, StandardCharsets.UTF_8);
                        }
                    }
                };
            case BOOLEAN:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) {
                        return object.booleanValue();
                    }

                    @Override
                    public Object convert(String object) {
                        return Boolean.parseBoolean(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride) {
                        return buffer.get() != 0;
                    }

                    @Override
                    public int bytesConsumed() {
                        return 1;
                    }
                };
            case BINARY:
            case VARBINARY:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.binaryValue();
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return java.util.Base64.getDecoder().decode(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        if (typeOverride != null) {
                            return typeOverride.reinterpretBytes(buffer, length, shape);
                        }
                        // If shape is set here, it should be [length], but we don't verify.
                        if (buffer.hasArray()
                                && buffer.arrayOffset() == 0
                                && buffer.position() == 0
                                && length == buffer.array().length) {
                            return buffer.array();
                        }
                        if (length < 0 || length > buffer.remaining()) {
                            throw new IOException(
                                    "ML Predict attempted to deserialize a byte array from a buffer with "
                                            + buffer.remaining()
                                            + " bytes remaining, but the length was "
                                            + length);
                        }
                        byte[] bytes = new byte[length];
                        buffer.get(bytes);
                        return bytes;
                    }
                };
            case DECIMAL:
                DecimalType decimalType = (DecimalType) targetType;
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.decimalValue();
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return new java.math.BigDecimal(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        byte[] bigEndian = new byte[length];
                        if (buffer.order() == ByteOrder.LITTLE_ENDIAN) {
                            for (int i = 0; i < length; i++) {
                                bigEndian[length - 1 - i] = buffer.get();
                            }
                        } else {
                            buffer.get(bigEndian);
                        }
                        return new BigDecimal(
                                new java.math.BigInteger(bigEndian), decimalType.getScale());
                    }
                };
            case TINYINT:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return (byte) object.shortValue();
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return Byte.parseByte(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        return buffer.get();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 1;
                    }
                };
            case SMALLINT:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.shortValue();
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return Short.parseShort(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        if (typeOverride != null) {
                            return typeOverride.reinterpretShort(buffer, length, shape);
                        }
                        return buffer.getShort();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 2;
                    }
                };
            case DATE:
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        if (!object.canConvertToInt()) {
                            // Int Overflow
                            throw new IOException(
                                    "ML Predict could not convert json field to int32, possible overflow.");
                        }
                        return object.intValue();
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return Integer.parseInt(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        if (typeOverride != null) {
                            return typeOverride.reinterpretInt(buffer, length, shape);
                        }
                        return buffer.getInt();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 4;
                    }
                };
            case BIGINT:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        if (!object.canConvertToLong()) {
                            // Int Overflow
                            throw new IOException(
                                    "ML Predict could not convert json field to int64, possible overflow.");
                        }
                        return object.longValue();
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return Long.parseLong(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        if (typeOverride != null) {
                            return typeOverride.reinterpretLong(buffer, length, shape);
                        }
                        return buffer.getLong();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 8;
                    }
                };
            case FLOAT:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.floatValue();
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return Float.parseFloat(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        if (typeOverride != null) {
                            return typeOverride.reinterpretFloat(buffer, length, shape);
                        }
                        return buffer.getFloat();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 4;
                    }
                };
            case DOUBLE:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.doubleValue();
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return Double.parseDouble(object);
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        if (typeOverride != null) {
                            return typeOverride.reinterpretDouble(buffer, length, shape);
                        }
                        return buffer.getDouble();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 8;
                    }
                };
            case ARRAY:
                return createArrayDeserializer((ArrayType) targetType);
            case ROW:
                return createRowDeserializer((RowType) targetType);
            case NULL:
                return new OutputDeserializer() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return null;
                    }

                    @Override
                    public Object convert(String object) throws IOException {
                        return null;
                    }

                    @Override
                    public Object convert(
                            ByteBuffer buffer,
                            int length,
                            List<Integer> shape,
                            ByteReinterpreter typeOverride)
                            throws IOException {
                        return null;
                    }
                };
            case MAP:
            case RAW:
            case SYMBOL:
            case MULTISET:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case UNRESOLVED:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type for model output: " + targetType);
        }
    }

    public static Class<?> toExternalConversionClass(LogicalType type) {
        // What data type do we use for arrays of each type?
        // Using primitives would be more efficient when these types are later converted to internal
        // types, but we use objects to support nulls.
        // There is likely some performance improvement to be had here.
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return String.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DECIMAL:
                return BigDecimal.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case ARRAY:
                // Recurse to find the nested type.
                return Array.newInstance(
                                toExternalConversionClass(((ArrayType) type).getElementType()), 0)
                        .getClass();
            case ROW:
                return Row.class;
            case NULL:
                return Object.class;
            default:
                throw new IllegalArgumentException("Unsupported type in ML_PREDICT: " + type);
        }
    }

    private static OutputDeserializer createArrayDeserializer(ArrayType targetType) {
        final OutputDeserializer elementConverter = createDeserializer(targetType.getElementType());
        final Class<?> elementClass = toExternalConversionClass(targetType.getElementType());
        return new OutputDeserializer() {
            @Override
            public Object convert(JsonNode object) throws IOException {
                final int length = object.size();
                final Object[] array = (Object[]) Array.newInstance(elementClass, length);
                for (int i = 0; i < length; ++i) {
                    array[i] = elementConverter.convert(object.get(i));
                }
                return array;
            }

            @Override
            public Object convert(JsonNode object, int offset, int length, List<Integer> shape)
                    throws IOException {
                if (length > object.size() - offset) {
                    throw new IOException(
                            "ML Predict attempted to deserialize an array of "
                                    + length
                                    + " objects from a json array of length "
                                    + (object.size() - offset));
                }
                List<Integer> childShape = null;
                int arrayLength = length;
                int objectsPerArray = 1;
                if (shape != null && !shape.isEmpty()) {
                    arrayLength = shape.get(0);
                    if (arrayLength <= 0) {
                        return (Object[]) Array.newInstance(elementClass, 0);
                    }
                    childShape = shape.subList(1, shape.size());
                    if (arrayLength > length) {
                        throw new IOException(
                                "ML Predict attempted to deserialize an array of length "
                                        + arrayLength
                                        + " from a json array of length "
                                        + length);
                    }
                    objectsPerArray = length / arrayLength;
                }

                final Object[] array = (Object[]) Array.newInstance(elementClass, arrayLength);
                for (int i = 0; i < arrayLength; ++i) {
                    array[i] =
                            elementConverter.convert(
                                    object,
                                    offset + i * objectsPerArray,
                                    objectsPerArray,
                                    childShape);
                }
                return array;
            }

            @Override
            public Object convert(String object) throws IOException {
                // Not supported.
                return null;
            }

            @Override
            public Object convert(List<String> list) throws IOException {
                final int length = list.size();
                final Object[] array = (Object[]) Array.newInstance(elementClass, length);
                for (int i = 0; i < length; ++i) {
                    array[i] = elementConverter.convert(list.get(i));
                }
                return array;
            }

            @Override
            public Object convert(
                    ByteBuffer buffer,
                    int length,
                    List<Integer> shape,
                    ByteReinterpreter typeOverride)
                    throws IOException {
                // Loop through the byte array and convert each element.
                if (length > buffer.remaining()) {
                    throw new IOException(
                            "ML Predict attempted to deserialize an array of "
                                    + length
                                    + " bytes from a byte array of length "
                                    + (buffer.remaining()));
                }
                List<Integer> childShape = null;
                int arrayLength;
                int bytesPerElement = elementConverter.bytesConsumed();
                if (typeOverride != null) {
                    bytesPerElement = typeOverride.bytesConsumed();
                }
                if (shape != null && !shape.isEmpty()) {
                    arrayLength = shape.get(0);
                    if (arrayLength <= 0) {
                        return (Object[]) Array.newInstance(elementClass, 0);
                    }
                    childShape = shape.subList(1, shape.size());
                    int minBytes =
                            bytesPerElement == 0 ? 1 : bytesPerElement == -1 ? 4 : bytesPerElement;
                    if (arrayLength * minBytes > length) {
                        throw new IOException(
                                "ML Predict attempted to deserialize an array of "
                                        + arrayLength * minBytes
                                        + " bytes from a byte array of length "
                                        + length);
                    }
                    if (bytesPerElement != -1) {
                        bytesPerElement = length / arrayLength;
                        // Decrease bytesPerElement to ensure it is a multiple of the minimum bytes
                        // consumed by the element converter. If the array is properly aligned, this
                        // will be a no-op.
                        bytesPerElement -= bytesPerElement % minBytes;
                    }

                } else {
                    if (bytesPerElement == 0) {
                        throw new UnsupportedOperationException(
                                "ML Predict attempted to deserialize an array of variable length types.");
                    }
                    arrayLength = length / bytesPerElement;
                }

                final Object[] array = (Object[]) Array.newInstance(elementClass, arrayLength);
                for (int i = 0; i < arrayLength; ++i) {
                    // Note that individual converters don't verify that they have enough bytes to
                    // consume, so we need to guarantee that here, which we have done by calculating
                    // the array length above.
                    int availableLength =
                            bytesPerElement > 0 ? bytesPerElement : buffer.remaining();
                    array[i] =
                            elementConverter.convert(
                                    buffer, availableLength, childShape, typeOverride);
                }
                return array;
            }

            @Override
            public int bytesConsumed() {
                // The minimum bytes consumed by an array is the bytes consumed by one element.
                return elementConverter.bytesConsumed();
            }
        };
    }

    private static OutputDeserializer createRowDeserializer(RowType rowType) {
        final OutputDeserializer[] fieldConverters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                idx -> {
                                    final RowType.RowField field = rowType.getFields().get(idx);
                                    return createDeserializer(field.getType());
                                })
                        .toArray(OutputDeserializer[]::new);
        final int arity = rowType.getFieldCount();
        return new OutputDeserializer() {
            @Override
            public Object convert(JsonNode object) throws IOException {
                Row row = new Row(arity);
                for (int i = 0; i < arity; ++i) {
                    final RowType.RowField field = rowType.getFields().get(i);
                    final JsonNode value = object.get(field.getName());
                    if (value != null) {
                        row.setField(i, fieldConverters[i].convert(value));
                    }
                }
                return row;
            }

            @Override
            public Object convert(JsonNode object, int offset, int length, List<Integer> shape)
                    throws IOException {
                JsonNode current = object;
                if (offset != 0) {
                    current = object.get(offset);
                }
                Row row = new Row(arity);
                for (int i = 0; i < arity; ++i) {
                    final RowType.RowField field = rowType.getFields().get(i);
                    final JsonNode value = current.get(field.getName());
                    if (value != null) {
                        row.setField(i, fieldConverters[i].convert(value, 0, value.size(), shape));
                    }
                }
                return row;
            }

            @Override
            public Object convert(String object) throws IOException {
                return null;
            }

            @Override
            public Object convert(List<String> list) throws IOException {
                Row row = new Row(list.size());
                for (int i = 0; i < list.size(); ++i) {
                    row.setField(i, fieldConverters[i].convert(list.get(i)));
                }
                return row;
            }

            @Override
            public Object convert(
                    ByteBuffer buffer,
                    int length,
                    List<Integer> shape,
                    ByteReinterpreter typeOverride)
                    throws IOException {
                return null;
            }
        };
    }

    public static ByteReinterpreter getByteReinterpreter(String dataType) {
        switch (dataType) {
            case "INT8":
                return new ByteReinterpreter() {
                    @Override
                    public short reinterpretShort(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        return buffer.get();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 1;
                    }
                };
            case "UINT8":
                return new ByteReinterpreter() {
                    @Override
                    public short reinterpretShort(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        // Unsigned, so we can't cast it to a byte, it has to be a short.
                        return (short) (buffer.get() & 0xFF);
                    }

                    @Override
                    public int bytesConsumed() {
                        return 1;
                    }
                };
            case "INT16":
                return new ByteReinterpreter() {
                    @Override
                    public short reinterpretShort(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        return buffer.getShort();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 2;
                    }
                };
            case "UINT16":
                return new ByteReinterpreter() {
                    @Override
                    public int reinterpretInt(ByteBuffer buffer, int length, List<Integer> shape) {
                        // Unsigned, so we can't cast it to a short.
                        return (int) (buffer.getShort() & 0xFFFF);
                    }

                    @Override
                    public int bytesConsumed() {
                        return 2;
                    }
                };
            case "INT32":
                return new ByteReinterpreter() {
                    @Override
                    public int reinterpretInt(ByteBuffer buffer, int length, List<Integer> shape) {
                        return buffer.getInt();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 4;
                    }
                };
            case "UINT32":
                return new ByteReinterpreter() {
                    @Override
                    public long reinterpretLong(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        // Since the int is unsigned, use it directly as the lower 32 bits of the
                        // long.
                        return buffer.getInt() & 0xFFFFFFFFL;
                    }

                    @Override
                    public int bytesConsumed() {
                        return 4;
                    }
                };
            case "INT64":
                return new ByteReinterpreter() {
                    @Override
                    public long reinterpretLong(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        return buffer.getLong();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 8;
                    }
                };
            case "UINT64":
                return new ByteReinterpreter() {
                    @Override
                    public long reinterpretLong(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        // Detect and throw if it's negative.
                        long l = buffer.getLong();
                        if (l < 0) {
                            throw new FlinkRuntimeException(
                                    "ML Predict overflowed when attempting to deserialize a signed "
                                            + "64-bit integer from an unsigned 64-bit integer.");
                        }
                        return l;
                    }

                    @Override
                    public int bytesConsumed() {
                        return 8;
                    }
                };
            case "FP16":
                return new ByteReinterpreter() {
                    @Override
                    public float reinterpretFloat(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        // Old java versions don't have support for half precision floats, new java
                        // versions don't really have it either. We have to do it ourselves with
                        // horrible bit manipulation.
                        short shortBits = buffer.getShort();
                        int mantissa = shortBits & 0x03ff; // 10 bits mantissa
                        int exponent = shortBits & 0x7c00; // 5 bits exponent
                        int sign = shortBits & 0x8000; // 1 bit sign
                        if (exponent == 0x7c00) {
                            exponent = 0x3fc00;
                        } else if (exponent != 0) {
                            exponent += 0x1c000;
                        } else if (mantissa != 0) {
                            // Exponent==0 and mantissa!=0 are subnormal numbers.
                            // We have to extend the mantissa by 13 bits to the left.
                            exponent = 0x1c400;
                            // normalize
                            do {
                                mantissa <<= 1; // multiply by 2
                                exponent -= 0x400; // decrease exponent by 1, i.e. divide by 2
                            } while ((mantissa & 0x400) == 0);
                            mantissa &= 0x3ff; // remove subnormal bits
                        }
                        return Float.intBitsToFloat(sign << 16 | (exponent | mantissa) << 13);
                    }

                    @Override
                    public int bytesConsumed() {
                        return 2;
                    }
                };
            case "FP32":
                return new ByteReinterpreter() {
                    @Override
                    public float reinterpretFloat(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        return buffer.getFloat();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 4;
                    }
                };
            case "FP64":
                return new ByteReinterpreter() {
                    @Override
                    public double reinterpretDouble(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        return buffer.getDouble();
                    }

                    @Override
                    public int bytesConsumed() {
                        return 8;
                    }
                };
            case "BF16":
                return new ByteReinterpreter() {
                    @Override
                    public float reinterpretFloat(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        // BFloat16 is a 16-bit floating point number, with 1 sign bit,
                        // 8 exponent bits, and 7 mantissa bits. Made specifically for
                        // machine learning.
                        short shortBits = buffer.getShort();
                        // Conversion to float is simpler than FP16, since we just have to pad
                        // it with 16 bits of 0s.
                        return Float.intBitsToFloat(shortBits << 16);
                    }

                    @Override
                    public int bytesConsumed() {
                        return 2;
                    }
                };
            case "BYTES":
                return new ByteReinterpreter() {
                    @Override
                    public byte[] reinterpretBytes(
                            ByteBuffer buffer, int length, List<Integer> shape) {
                        // The first four bytes are a little-endian int32 that tells us the length
                        // of the byte array.
                        int arrayLength = buffer.getInt();
                        if (arrayLength < 0 || arrayLength > buffer.remaining()) {
                            throw new FlinkRuntimeException(
                                    "ML Predict attempted to deserialize a byte array of length "
                                            + arrayLength
                                            + " from a byte array of length "
                                            + (buffer.remaining()));
                        }
                        byte[] bytes = new byte[arrayLength];
                        buffer.get(bytes);
                        return bytes;
                    }

                    @Override
                    public int bytesConsumed() {
                        // variable length
                        return -1;
                    }
                };
            default:
                return null;
        }
    }
}
