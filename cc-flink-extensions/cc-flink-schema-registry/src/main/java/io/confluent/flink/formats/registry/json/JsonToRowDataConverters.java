/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.flink.formats.converters.json.CombinedSchemaUtils;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Runtime converters between {@link JsonNode} and {@link org.apache.flink.table.data.RowData}. */
public class JsonToRowDataConverters {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    /**
     * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface JsonToRowDataConverter extends Serializable {

        Object convert(JsonNode object) throws IOException;
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
    public static JsonToRowDataConverter createConverter(
            Schema readSchema, LogicalType targetType) {
        if (targetType.isNullable()) {
            final Schema nonNullableSchema =
                    ConverterUtils.extractNonNullableSchemaFromOneOf(readSchema);
            final JsonToRowDataConverter nonNullConverter =
                    createNonNullConverterWithReferenceSchema(nonNullableSchema, targetType);
            return new JsonToRowDataConverter() {
                @Override
                public Object convert(JsonNode object) throws IOException {
                    if (object.isNull()) {
                        return null;
                    } else {
                        return nonNullConverter.convert(object);
                    }
                }
            };
        } else {
            return createNonNullConverterWithReferenceSchema(readSchema, targetType);
        }
    }

    private static JsonToRowDataConverter createNonNullConverterWithReferenceSchema(
            Schema readSchema, LogicalType targetType) {
        if (readSchema instanceof ReferenceSchema) {
            return createNonNullConverterWithReferenceSchema(
                    ((ReferenceSchema) readSchema).getReferredSchema(), targetType);
        } else {
            return createNonNullConverter(readSchema, targetType);
        }
    }

    private static JsonToRowDataConverter createNonNullConverter(
            Schema readSchema, LogicalType targetType) {
        switch (targetType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) {
                        return StringData.fromString(object.textValue());
                    }
                };
            case BOOLEAN:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) {
                        return object.booleanValue();
                    }
                };
            case BINARY:
            case VARBINARY:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.binaryValue();
                    }
                };
            case DECIMAL:
                DecimalType decimalType = (DecimalType) targetType;
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return DecimalData.fromBigDecimal(
                                object.decimalValue(),
                                decimalType.getPrecision(),
                                decimalType.getScale());
                    }
                };
            case TINYINT:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return (byte) object.shortValue();
                    }
                };
            case SMALLINT:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.shortValue();
                    }
                };
            case DATE:
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.intValue();
                    }
                };
            case BIGINT:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.longValue();
                    }
                };
            case FLOAT:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.floatValue();
                    }
                };
            case DOUBLE:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return object.doubleValue();
                    }
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final LocalZonedTimestampType timestampType = (LocalZonedTimestampType) targetType;
                return createTimestampConverter(timestampType.getPrecision());
            case ARRAY:
                return createArrayConverter(readSchema, (ArrayType) targetType);
            case MAP:
                return createMapConverter(readSchema, (MapType) targetType);
            case ROW:
                return createRowConverter(readSchema, (RowType) targetType);
            case NULL:
                return new JsonToRowDataConverter() {
                    @Override
                    public Object convert(JsonNode object) throws IOException {
                        return null;
                    }
                };
            case RAW:
            case SYMBOL:
            case MULTISET:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case UNRESOLVED:
            default:
                throw new IllegalStateException(
                        "Couldn't translate unsupported type " + targetType.getTypeRoot() + ".");
        }
    }

    private static JsonToRowDataConverter createRowConverter(Schema readSchema, RowType rowType) {

        if (readSchema instanceof CombinedSchema) {
            final CombinedSchema combinedSchema = (CombinedSchema) readSchema;
            if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
                final Schema transformedSchema =
                        CombinedSchemaUtils.transformedSchema(combinedSchema);
                return createRowConverter(transformedSchema, rowType);
            } else if (combinedSchema.getCriterion() == CombinedSchema.ONE_CRITERION
                    || combinedSchema.getCriterion() == CombinedSchema.ANY_CRITERION) {
                return createUnionRowConverter(readSchema, rowType);
            } else {
                throw new IllegalStateException(
                        "Unknown criterion: " + combinedSchema.getCriterion());
            }
        } else {
            return createRegularRowConverter(readSchema, rowType);
        }
    }

    private static JsonToRowDataConverter createUnionRowConverter(
            Schema readSchema, RowType rowType) {

        final int arity = rowType.getFieldCount();
        final List<Schema> subschemas =
                new ArrayList<>(((CombinedSchema) readSchema).getSubschemas());
        final JsonToRowDataConverter[] fieldConverters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                idx -> createConverter(subschemas.get(idx), rowType.getTypeAt(idx)))
                        .toArray(JsonToRowDataConverter[]::new);
        return new JsonToRowDataConverter() {
            @Override
            public Object convert(JsonNode object) throws IOException {
                int numMatchingProperties = -1;
                int matchingFieldIdx = -1;
                final GenericRowData result = new GenericRowData(arity);
                for (int i = 0; i < arity; i++) {
                    final LogicalType fieldType = rowType.getFields().get(i).getType();
                    if (matchesSimpleType(fieldType, object)) {
                        result.setField(i, fieldConverters[i].convert(object));
                        return result;
                    } else if (fieldType.is(LogicalTypeRoot.ROW)) {
                        int matching = matchStructSchema((RowType) fieldType, object);
                        if (matching > numMatchingProperties) {
                            numMatchingProperties = matching;
                            matchingFieldIdx = i;
                        }
                    }
                }
                if (matchingFieldIdx != -1) {
                    result.setField(
                            matchingFieldIdx, fieldConverters[matchingFieldIdx].convert(object));
                    return result;
                }

                throw new IllegalStateException("Did not find matching oneof field for data");
            }
        };
    }

    private static int matchStructSchema(RowType fieldType, JsonNode jsonNode) {
        Set<String> schemaFields =
                fieldType.getFields().stream().map(RowField::getName).collect(Collectors.toSet());
        Set<String> objectFields = new HashSet<>();
        for (Iterator<Entry<String, JsonNode>> iter = jsonNode.fields(); iter.hasNext(); ) {
            objectFields.add(iter.next().getKey());
        }
        Set<String> intersectSet = new HashSet<>(schemaFields);
        intersectSet.retainAll(objectFields);
        return intersectSet.size();
    }

    private static boolean matchesSimpleType(LogicalType targetType, JsonNode jsonNode) {
        switch (targetType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return jsonNode.isTextual();
            case BOOLEAN:
                return jsonNode.isBoolean();
            case BINARY:
            case VARBINARY:
                return jsonNode.isBinary();
            case DECIMAL:
                return jsonNode.isBigDecimal();
            case TINYINT:
                return jsonNode.isIntegralNumber();
            case SMALLINT:
                return jsonNode.isShort();
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTEGER:
                return jsonNode.isInt();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case BIGINT:
                return jsonNode.isLong();
            case FLOAT:
                return jsonNode.isFloat();
            case DOUBLE:
                return jsonNode.isDouble();
            case ARRAY:
                return jsonNode.isArray();
            case MAP:
                return jsonNode.isObject() || jsonNode.isArray();
            case ROW:
            case MULTISET:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case NULL:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            default:
                return false;
        }
    }

    private static JsonToRowDataConverter createRegularRowConverter(
            Schema readSchema, RowType rowType) {
        final ObjectSchema objectSchema = (ObjectSchema) readSchema;
        final JsonToRowDataConverter[] fieldConverters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                idx -> {
                                    final RowField field = rowType.getFields().get(idx);
                                    return createConverter(
                                            objectSchema.getPropertySchemas().get(field.getName()),
                                            field.getType());
                                })
                        .toArray(JsonToRowDataConverter[]::new);
        final int arity = rowType.getFieldCount();
        return new JsonToRowDataConverter() {
            @Override
            public Object convert(JsonNode object) throws IOException {
                GenericRowData row = new GenericRowData(arity);
                for (int i = 0; i < arity; ++i) {
                    final RowField field = rowType.getFields().get(i);
                    final JsonNode value = object.get(field.getName());
                    if (value != null) {
                        row.setField(i, fieldConverters[i].convert(value));
                    }
                }
                return row;
            }
        };
    }

    private static JsonToRowDataConverter createMapConverter(Schema readSchema, MapType mapType) {
        final LogicalType keyType = mapType.getKeyType();
        final LogicalType valueType = mapType.getValueType();

        if (keyType.is(LogicalTypeFamily.CHARACTER_STRING) && !keyType.isNullable()) {
            final ObjectSchema objectSchema = (ObjectSchema) readSchema;
            final JsonToRowDataConverter valueConverter =
                    createConverter(objectSchema.getSchemaOfAdditionalProperties(), valueType);
            return new JsonToRowDataConverter() {
                @Override
                public Object convert(JsonNode object) throws IOException {
                    Map<Object, Object> result = new HashMap<>();
                    Iterator<Entry<String, JsonNode>> fieldIt = object.fields();
                    while (fieldIt.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fieldIt.next();
                        result.put(
                                StringData.fromString(entry.getKey()),
                                valueConverter.convert(entry.getValue()));
                    }
                    return new GenericMapData(result);
                }
            };
        } else {
            final ObjectSchema mapEntrySchema =
                    (ObjectSchema) ((ArraySchema) readSchema).getAllItemSchema();
            final Map<String, Schema> propertySchemas = mapEntrySchema.getPropertySchemas();
            final JsonToRowDataConverter keyConverter =
                    createConverter(propertySchemas.get(KEY_FIELD), keyType);
            final JsonToRowDataConverter valueConverter =
                    createConverter(propertySchemas.get(VALUE_FIELD), valueType);
            return new JsonToRowDataConverter() {
                @Override
                public Object convert(JsonNode object) throws IOException {
                    Map<Object, Object> result = new HashMap<>();
                    for (JsonNode entry : object) {
                        result.put(
                                keyConverter.convert(entry.get(KEY_FIELD)),
                                valueConverter.convert(entry.get(VALUE_FIELD)));
                    }
                    return new GenericMapData(result);
                }
            };
        }
    }

    private static JsonToRowDataConverter createArrayConverter(
            Schema readSchema, ArrayType targetType) {
        final ArraySchema arraySchema = (ArraySchema) readSchema;
        final JsonToRowDataConverter elementConverter =
                createConverter(arraySchema.getAllItemSchema(), targetType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(targetType.getElementType());
        return new JsonToRowDataConverter() {
            @Override
            public Object convert(JsonNode object) throws IOException {
                final int length = object.size();
                final Object[] array = (Object[]) Array.newInstance(elementClass, length);
                for (int i = 0; i < length; ++i) {
                    array[i] = elementConverter.convert(object.get(i));
                }
                return new GenericArrayData(array);
            }
        };
    }

    private static JsonToRowDataConverter createTimestampConverter(int precision) {
        if (precision <= 3) {
            return new JsonToRowDataConverter() {
                @Override
                public Object convert(JsonNode object) {
                    return TimestampData.fromEpochMillis(object.longValue());
                }
            };
        } else if (precision <= 6) {
            return new JsonToRowDataConverter() {
                @Override
                public Object convert(JsonNode object) {
                    final long micros = object.longValue();
                    final long millis = micros / 1000;
                    final long nanosOfMilli = micros % 1_000 * 1_000;
                    return TimestampData.fromEpochMillis(millis, (int) nanosOfMilli);
                }
            };
        } else {
            throw new IllegalStateException("Unsupported precision: " + precision);
        }
    }
}
