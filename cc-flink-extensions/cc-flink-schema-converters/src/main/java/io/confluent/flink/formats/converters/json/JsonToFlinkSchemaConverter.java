/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

import org.apache.flink.annotation.Confluent;
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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_INDEX_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_PARAMETERS;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_BYTES;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_DATE;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_DECIMAL;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_DECIMAL_PRECISION;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_DECIMAL_SCALE;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_FLOAT32;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_FLOAT64;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_INT16;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_INT32;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_INT64;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_INT8;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_MAP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_TIME;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_TIMESTAMP;
import static io.confluent.flink.formats.converters.json.CommonConstants.KEY_FIELD;
import static io.confluent.flink.formats.converters.json.CommonConstants.VALUE_FIELD;

/**
 * A converter from {@link Schema} to {@link LogicalType}.
 *
 * <p>The mapping is represented by the following table:
 *
 * <pre>
 * +-------------------------------------+-------------------------+-----------------------------------------+-----------------+
 * |              Json type              | Connect type annotation |               Type title                |   Flink type    |
 * +-------------------------------------+-------------------------+-----------------------------------------+-----------------+
 * | BooleanSchema                       |                         |                                         | BOOLEAN         |
 * | NumberSchema(requiresInteger=true)  |                         |                                         | BIGINT          |
 * | NumberSchema(requiresInteger=false) |                         |                                         | DOUBLE          |
 * | NumberSchema                        | int8                    |                                         | TINYINT         |
 * | NumberSchema                        | int16                   |                                         | SMALLINT        |
 * | NumberSchema                        | int32                   |                                         | INT             |
 * | NumberSchema                        | int32                   | org.apache.kafka.connect.data.Time      | TIME(3)         |
 * | NumberSchema                        | int32                   | org.apache.kafka.connect.data.Date      | DATE            |
 * | NumberSchema                        | int64                   |                                         | BIGINT          |
 * | NumberSchema                        | int64                   | org.apache.kafka.connect.data.Timestamp | TIMESTAMP_LTZ(3)|
 * | NumberSchema                        | float32                 |                                         | FLOAT           |
 * | NumberSchema                        | float64                 |                                         | DOUBLE          |
 * | NumberSchema                        | bytes                   | org.apache.kafka.connect.data.Decimal   | DECIMAL         |
 * | StringSchema                        | bytes                   |                                         | VARBINARY       |
 * | StringSchema                        |                         |                                         | VARCHAR         |
 * | EnumSchema                          |                         |                                         | VARCHAR         |
 * | CombinedSchema                      |                         |                                         | ROW             |
 * | ArraySchema                         | map                     |                                         | MAP[K, V]       |
 * | ArraySchema                         |                         |                                         | ARRAY           |
 * | ObjectSchema                        | map                     |                                         | MAP[VARCHAR, V] |
 * | ObjectSchema                        |                         |                                         | ROW             |
 * +-------------------------------------+-------------------------+-----------------------------------------+-----------------+
 * </pre>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>nullable types are expressed as oneOf(NullSchema, T)
 *   <li>ObjectSchema for a MAP and MULTISET must have two fields [key, value]
 *   <li>CombinedSchema (oneOf, allOf, anyOf) is expressed as a ROW, unless it can be simplified
 *       (e.g. StringSchema and ConstantSchema)
 * </ul>
 */
@Confluent
public class JsonToFlinkSchemaConverter {

    /**
     * Mostly adapted the logic from <a
     * href="https://github.com/confluentinc/schema-registry/blob/dd18837717ac3cc2ba8c4574cbff09d95ff17b66/json-schema-converter/src/main/java/io/confluent/connect/json/JsonSchemaData.java">JsonSchemaData</a>
     * Should be kept in sync to handle all connect data types.
     */
    public static LogicalType toFlinkSchema(final Schema schema) {
        final CycleContext context = new CycleContext();
        return toFlinkSchemaWithCycleDetection(schema, false, context);
    }

    private static LogicalType toFlinkSchemaWithCycleDetection(
            final Schema schema, boolean isOptional, CycleContext cycleContext) {

        final Schema toTranslate;
        if (schema instanceof ReferenceSchema) {
            final Schema referredSchema = ((ReferenceSchema) schema).getReferredSchema();
            if (cycleContext.seenSchemas.contains(referredSchema)) {
                throw new IllegalArgumentException("Cyclic schemas are not supported.");
            }
            cycleContext.seenSchemas.add(referredSchema);
            toTranslate = referredSchema;
        } else {
            toTranslate = schema;
        }
        final LogicalType result = toFlinkSchema(toTranslate, isOptional, cycleContext);
        if (schema instanceof ReferenceSchema) {
            cycleContext.seenSchemas.remove(((ReferenceSchema) schema).getReferredSchema());
        }
        return result;
    }

    private static LogicalType toFlinkSchema(
            final Schema schema, boolean isOptional, CycleContext cycleContext) {
        final String title = schema.getTitle();
        if (schema instanceof BooleanSchema) {
            return new BooleanType(isOptional);
        } else if (schema instanceof NumberSchema) {
            NumberSchema numberSchema = (NumberSchema) schema;
            String type = (String) numberSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (type == null) {
                return numberSchema.requiresInteger()
                        ? new BigIntType(isOptional)
                        : new DoubleType(isOptional);
            } else {
                switch (type) {
                    case CONNECT_TYPE_INT8:
                        return new TinyIntType(isOptional);
                    case CONNECT_TYPE_INT16:
                        return new SmallIntType(isOptional);
                    case CONNECT_TYPE_INT32:
                        return fromInt32Type(isOptional, title);
                    case CONNECT_TYPE_INT64:
                        return fromInt64Type(isOptional, title);
                    case CONNECT_TYPE_FLOAT32:
                        return new FloatType(isOptional);
                    case CONNECT_TYPE_FLOAT64:
                        return new DoubleType(isOptional);
                    case CONNECT_TYPE_BYTES: // decimal logical type
                        if (!CONNECT_TYPE_DECIMAL.equals(title)) {
                            throw new IllegalArgumentException("Expected decimal type");
                        }
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> properties =
                                (Map<String, Object>)
                                        schema.getUnprocessedProperties()
                                                .getOrDefault(
                                                        CONNECT_PARAMETERS, Collections.emptyMap());
                        final int scale =
                                Optional.ofNullable(properties.get(CONNECT_TYPE_DECIMAL_SCALE))
                                        .map(prop -> Integer.parseInt((String) prop))
                                        .orElse(DecimalType.DEFAULT_SCALE);
                        final int precision =
                                Optional.ofNullable(properties.get(CONNECT_TYPE_DECIMAL_PRECISION))
                                        .map(prop -> Integer.parseInt((String) prop))
                                        .orElse(DecimalType.DEFAULT_PRECISION);
                        return new DecimalType(isOptional, precision, scale);
                    default:
                        throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        } else if (schema instanceof StringSchema) {
            String type = (String) schema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            return CONNECT_TYPE_BYTES.equals(type)
                    ? new VarBinaryType(isOptional, VarBinaryType.MAX_LENGTH)
                    : new VarCharType(isOptional, VarCharType.MAX_LENGTH);
        } else if (schema instanceof EnumSchema) {
            // enums are unwrapped to strings and the original enum is not preserved
            return new VarCharType(isOptional, VarCharType.MAX_LENGTH);
        } else if (schema instanceof CombinedSchema) {
            CombinedSchema combinedSchema = (CombinedSchema) schema;
            CombinedSchema.ValidationCriterion criterion = combinedSchema.getCriterion();
            if (criterion == CombinedSchema.ALL_CRITERION) {
                return fromAllOfSchema(combinedSchema, isOptional, cycleContext);
            } else if (criterion != CombinedSchema.ONE_CRITERION
                    && criterion != CombinedSchema.ANY_CRITERION) {
                throw new IllegalArgumentException("Unsupported criterion: " + criterion);
            }
            if (combinedSchema.getSubschemas().size() == 2) {
                boolean foundNullSchema = false;
                org.everit.json.schema.Schema nonNullSchema = null;
                for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
                    if (subSchema instanceof NullSchema) {
                        foundNullSchema = true;
                    } else {
                        nonNullSchema = subSchema;
                    }
                }
                if (foundNullSchema) {
                    return toFlinkSchemaWithCycleDetection(nonNullSchema, true, cycleContext);
                }
            }
            int index = 0;
            boolean isOptionalRow = isOptional;
            final List<RowField> fields = new ArrayList<>();
            for (Schema subSchema : combinedSchema.getSubschemas()) {
                if (subSchema instanceof NullSchema) {
                    isOptionalRow = true;
                } else {
                    String subFieldName = CommonConstants.GENERALIZED_TYPE_UNION_PREFIX + index;
                    fields.add(
                            new RowField(
                                    subFieldName,
                                    toFlinkSchemaWithCycleDetection(
                                            subSchema, true, cycleContext)));
                    index++;
                }
            }
            return new RowType(isOptionalRow, fields);
        } else if (schema instanceof ArraySchema) {
            ArraySchema arraySchema = (ArraySchema) schema;
            org.everit.json.schema.Schema itemsSchema = arraySchema.getAllItemSchema();
            if (itemsSchema == null) {
                throw new IllegalArgumentException("Array schema did not specify the items type");
            }
            String type = (String) arraySchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_MAP.equals(type) && itemsSchema instanceof ObjectSchema) {
                ObjectSchema objectSchema = (ObjectSchema) itemsSchema;
                return new MapType(
                        isOptional,
                        toFlinkSchemaWithCycleDetection(
                                objectSchema.getPropertySchemas().get(KEY_FIELD),
                                false,
                                cycleContext),
                        toFlinkSchemaWithCycleDetection(
                                objectSchema.getPropertySchemas().get(VALUE_FIELD),
                                false,
                                cycleContext));
            } else {
                return new ArrayType(
                        isOptional,
                        toFlinkSchemaWithCycleDetection(itemsSchema, false, cycleContext));
            }
        } else if (schema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) schema;
            String type = (String) objectSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_MAP.equals(type)) {
                return new MapType(
                        isOptional,
                        new VarCharType(false, VarCharType.MAX_LENGTH),
                        toFlinkSchemaWithCycleDetection(
                                objectSchema.getSchemaOfAdditionalProperties(),
                                false,
                                cycleContext));
            } else {
                Map<String, Schema> properties = objectSchema.getPropertySchemas();
                SortedMap<Integer, Entry<String, Schema>> sortedMap = new TreeMap<>();
                for (Map.Entry<String, org.everit.json.schema.Schema> property :
                        properties.entrySet()) {
                    org.everit.json.schema.Schema subSchema = property.getValue();
                    Integer index =
                            (Integer) subSchema.getUnprocessedProperties().get(CONNECT_INDEX_PROP);
                    if (index == null) {
                        index = sortedMap.size();
                    }
                    sortedMap.put(index, property);
                }
                final List<RowField> rowFields =
                        sortedMap.values().stream()
                                .map(
                                        property -> {
                                            String subFieldName = property.getKey();
                                            Schema subSchema = property.getValue();
                                            boolean isFieldOptional =
                                                    !objectSchema
                                                            .getRequiredProperties()
                                                            .contains(subFieldName);
                                            return new RowField(
                                                    subFieldName,
                                                    toFlinkSchemaWithCycleDetection(
                                                            subSchema,
                                                            isFieldOptional,
                                                            cycleContext),
                                                    schema.getDescription());
                                        })
                                .collect(Collectors.toList());
                return new RowType(isOptional, rowFields);
            }
        } else if (schema instanceof ReferenceSchema) {
            return toFlinkSchemaWithCycleDetection(
                    ((ReferenceSchema) schema).getReferredSchema(), isOptional, cycleContext);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported schema type " + schema.getClass().getName());
        }
    }

    private static LogicalType fromInt64Type(boolean isOptional, String title) {
        if (Objects.equals(title, CONNECT_TYPE_TIMESTAMP)) {
            return new LocalZonedTimestampType(isOptional, 3);
        }
        return new BigIntType(isOptional);
    }

    private static LogicalType fromInt32Type(boolean isOptional, String title) {
        if (Objects.equals(title, CONNECT_TYPE_TIME)) {
            return new TimeType(isOptional, 3);
        } else if (Objects.equals(title, CONNECT_TYPE_DATE)) {
            return new DateType(isOptional);
        } else {
            return new IntType(isOptional);
        }
    }

    private static LogicalType fromAllOfSchema(
            CombinedSchema combinedSchema, boolean isOptional, CycleContext cycleContext) {
        return toFlinkSchemaWithCycleDetection(
                CombinedSchemaUtils.transformedSchema(combinedSchema), isOptional, cycleContext);
    }

    private static final class CycleContext {

        private final Set<Schema> seenSchemas = new HashSet<>();
    }
}
