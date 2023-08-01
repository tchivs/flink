/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ArrayData.ElementGetter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.flink.formats.converters.json.CombinedSchemaUtils;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** Tool class used to convert from {@link RowData} to {@link JsonNode}. * */
@Confluent
public class RowDataToJsonConverters {

    // --------------------------------------------------------------------------------
    // Runtime Converters
    // --------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding {@link JsonNode}s.
     */
    @FunctionalInterface
    public interface RowDataToJsonConverter extends Serializable {

        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
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
    public static RowDataToJsonConverter createConverter(LogicalType type, Schema targetSchema) {

        if (type.isNullable()) {
            final Schema nonNullType = ConverterUtils.extractNonNullableSchema(targetSchema);
            final RowDataToJsonConverter converter = createConverterNotNull(type, nonNullType);

            // wrap into nullable converter
            return new RowDataToJsonConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                    if (value == null) {
                        return mapper.nullNode();
                    }

                    return converter.convert(mapper, reuse, value);
                }
            };
        } else {
            return createConverterNotNull(type, targetSchema);
        }
    }

    private static RowDataToJsonConverter createConverterNotNull(
            LogicalType type, Schema targetSchema) {
        switch (type.getTypeRoot()) {
            case NULL:
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.nullNode();
                    }
                };
            case TINYINT:
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        // Cast to short to create a ShortNode, otherwise an IntNode will be created
                        return mapper.getNodeFactory().numberNode((short) (byte) value);
                    }
                };
            case SMALLINT:
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((short) value);
                    }
                };
            case BOOLEAN: // boolean
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().booleanNode((boolean) value);
                    }
                };
            case INTEGER: // int
            case TIME_WITHOUT_TIME_ZONE: // int
            case DATE: // int
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((int) value);
                    }
                };
            case BIGINT: // long
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((long) value);
                    }
                };
            case FLOAT: // float
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((float) value);
                    }
                };
            case DOUBLE: // double
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((double) value);
                    }
                };
            case CHAR:
            case VARCHAR:
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().textNode(value.toString());
                    }
                };
            case BINARY:
            case VARBINARY:
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().binaryNode((byte[]) value);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final TimestampType timestampType = (TimestampType) type;
                return createTimestampConverter(timestampType.getPrecision());
            case DECIMAL:
                return new RowDataToJsonConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        final DecimalData decimalData = (DecimalData) value;
                        return mapper.getNodeFactory().numberNode(decimalData.toBigDecimal());
                    }
                };
            case ARRAY:
                return createArrayConverter((ArrayType) type, targetSchema);
            case ROW:
                return createRowConverter((RowType) type, targetSchema);
            case MAP:
                return createMapConverter((MapType) type, targetSchema);
            case MULTISET:
            case RAW:
            case INTERVAL_YEAR_MONTH: // long
            case INTERVAL_DAY_TIME: // long
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static RowDataToJsonConverter createTimestampConverter(int precision) {
        if (precision <= 3) {
            return new RowDataToJsonConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                    return mapper.getNodeFactory()
                            .numberNode(((TimestampData) value).toInstant().toEpochMilli());
                }
            };
        } else if (precision <= 6) {
            return new RowDataToJsonConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                    // copied over from
                    // org.apache.avro.data.TimeConversions.TimestampMicrosConversion.toLong
                    final Instant instant = ((TimestampData) value).toInstant();
                    long seconds = instant.getEpochSecond();
                    int nanos = instant.getNano();

                    final long result;
                    if (seconds < 0 && nanos > 0) {
                        long micros = Math.multiplyExact(seconds + 1, 1_000_000L);
                        long adjustment = (nanos / 1_000L) - 1_000_000;

                        result = Math.addExact(micros, adjustment);
                    } else {
                        long micros = Math.multiplyExact(seconds, 1_000_000L);

                        result = Math.addExact(micros, nanos / 1_000L);
                    }
                    return mapper.getNodeFactory().numberNode(result);
                }
            };
        } else {
            throw new IllegalStateException("Unsupported timestamp precision: " + precision);
        }
    }

    private static RowDataToJsonConverter createRowConverter(RowType rowType, Schema targetSchema) {
        if (targetSchema instanceof CombinedSchema) {
            final CombinedSchema combinedSchema = (CombinedSchema) targetSchema;
            if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
                final Schema transformedSchema =
                        CombinedSchemaUtils.transformedSchema(combinedSchema);
                return createRowConverter(rowType, transformedSchema);
            } else if (combinedSchema.getCriterion() == CombinedSchema.ONE_CRITERION
                    || combinedSchema.getCriterion() == CombinedSchema.ANY_CRITERION) {
                return createRowToUnionConverter(rowType, combinedSchema);
            } else {
                throw new IllegalStateException(
                        "Unknown criterion: " + combinedSchema.getCriterion());
            }
        } else {
            return createRowToObjectConverter(rowType, (ObjectSchema) targetSchema);
        }
    }

    private static RowDataToJsonConverter createRowToUnionConverter(
            RowType rowType, CombinedSchema targetSchema) {
        final int arity = rowType.getFieldCount();
        final List<Schema> subschemas = new ArrayList<>(targetSchema.getSubschemas());
        final RowDataToJsonConverter[] fieldConverters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                idx -> createConverter(rowType.getTypeAt(idx), subschemas.get(idx)))
                        .toArray(RowDataToJsonConverter[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < arity; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return new RowDataToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                final RowData row = (RowData) value;
                int nonNullField = -1;
                for (int i = 0; i < fieldGetters.length; i++) {
                    if (!row.isNullAt(i)) {
                        nonNullField = i;
                        break;
                    }
                }

                if (nonNullField == -1) {
                    return mapper.nullNode();
                } else {
                    return fieldConverters[nonNullField].convert(
                            mapper, null, fieldGetters[nonNullField].getFieldOrNull(row));
                }
            }
        };
    }

    private static RowDataToJsonConverter createRowToObjectConverter(
            RowType rowType, ObjectSchema targetSchema) {
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        final Map<String, Schema> propertySchemas = targetSchema.getPropertySchemas();
        final RowDataToJsonConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(
                                field ->
                                        RowDataToJsonConverters.createConverter(
                                                field.getType(),
                                                propertySchemas.get(field.getName())))
                        .toArray(RowDataToJsonConverter[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }
        final int length = rowType.getFieldCount();
        final List<String> requiredProperties = targetSchema.getRequiredProperties();

        return new RowDataToJsonConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ObjectNode node;
                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createObjectNode();
                } else {
                    node = (ObjectNode) reuse;
                }
                final RowData row = (RowData) value;
                for (int i = 0; i < length; ++i) {
                    String fieldName = fieldNames[i];
                    try {
                        Object field = fieldGetters[i].getFieldOrNull(row);
                        if (field != null || requiredProperties.contains(fieldName)) {
                            node.set(
                                    fieldName,
                                    fieldConverters[i].convert(mapper, node.get(fieldName), field));
                        }
                    } catch (Throwable t) {
                        throw new RuntimeException(
                                String.format("Fail to serialize at field: %s.", fieldName), t);
                    }
                }
                return node;
            }
        };
    }

    private static RowDataToJsonConverter createArrayConverter(
            ArrayType arrayType, Schema targetSchema) {
        final ArraySchema arraySchema = (ArraySchema) targetSchema;
        LogicalType elementType = arrayType.getElementType();
        final ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        final RowDataToJsonConverter elementConverter =
                createConverter(arrayType.getElementType(), arraySchema.getAllItemSchema());

        return new RowDataToJsonConverter() {
            private static final long serialVersionUID = 1L;

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

                ArrayData array = (ArrayData) value;
                int numElements = array.size();
                for (int i = 0; i < numElements; i++) {
                    Object element = elementGetter.getElementOrNull(array, i);
                    node.add(elementConverter.convert(mapper, null, element));
                }

                return node;
            }
        };
    }

    //  private static RowDataToAvroConverter createMultisetConverter(
    //      MultisetType type, Schema targetSchema) {
    //    final LogicalType keyType = type.getElementType();
    //    final LogicalType valueType = new IntType(false);
    //    final ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
    //    final ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
    //
    //    return createMapConverter(targetSchema, keyType, valueType, valueGetter, keyGetter);
    //  }

    private static RowDataToJsonConverter createMapConverter(MapType type, Schema targetSchema) {
        final LogicalType keyType = type.getKeyType();
        final LogicalType valueType = type.getValueType();
        final ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        final ElementGetter keyGetter = ArrayData.createElementGetter(keyType);

        return createMapConverter(targetSchema, keyType, valueType, valueGetter, keyGetter);
    }

    private static RowDataToJsonConverter createMapConverter(
            Schema targetSchema,
            LogicalType keyType,
            LogicalType valueType,
            ElementGetter valueGetter,
            ElementGetter keyGetter) {
        if (targetSchema instanceof ObjectSchema) {
            final RowDataToJsonConverter valueConverter =
                    createConverter(
                            valueType,
                            ((ObjectSchema) targetSchema).getSchemaOfAdditionalProperties());
            return new RowDataToJsonConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                    final ObjectNode node;
                    // reuse could be a NullNode if last record is null.
                    if (reuse == null || reuse.isNull()) {
                        node = mapper.createObjectNode();
                    } else {
                        node = (ObjectNode) reuse;
                        node.removeAll();
                    }

                    MapData map = (MapData) value;
                    ArrayData keyArray = map.keyArray();
                    ArrayData valueArray = map.valueArray();
                    int numElements = map.size();
                    for (int i = 0; i < numElements; i++) {
                        String fieldName = keyArray.getString(i).toString();

                        Object element = valueGetter.getElementOrNull(valueArray, i);
                        node.set(
                                fieldName,
                                valueConverter.convert(mapper, node.get(fieldName), element));
                    }

                    return node;
                }
            };
        } else if (targetSchema instanceof ArraySchema) {
            final ObjectSchema elementType =
                    (ObjectSchema) ((ArraySchema) targetSchema).getAllItemSchema();
            final Map<String, Schema> propertySchemas = elementType.getPropertySchemas();
            final RowDataToJsonConverter keyConverter =
                    createConverter(keyType, propertySchemas.get("key"));
            final RowDataToJsonConverter valueConverter =
                    createConverter(valueType, propertySchemas.get("value"));
            return new RowDataToJsonConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                    final MapData mapData = (MapData) value;
                    final ArrayData keyArray = mapData.keyArray();
                    final ArrayData valueArray = mapData.valueArray();

                    final ArrayNode node;

                    // reuse could be a NullNode if last record is null.
                    if (reuse == null || reuse.isNull()) {
                        node = mapper.createArrayNode();
                    } else {
                        node = (ArrayNode) reuse;
                        node.removeAll();
                    }

                    for (int i = 0; i < mapData.size(); i++) {
                        final ObjectNode elementNode = mapper.createObjectNode();
                        final JsonNode key =
                                keyConverter.convert(
                                        mapper, null, keyGetter.getElementOrNull(keyArray, i));
                        final JsonNode entryValue =
                                valueConverter.convert(
                                        mapper, null, valueGetter.getElementOrNull(valueArray, i));
                        elementNode.set("key", key);
                        elementNode.set("value", entryValue);
                        node.add(elementNode);
                    }

                    return node;
                }
            };
        } else {
            throw new IllegalStateException("Illegal JSON type for a MAP type: " + targetSchema);
        }
    }
}
