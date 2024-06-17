/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_PRECISION;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_MULTISET;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_PROP;
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
            if (cycleContext.seenSchemas.contains(referredSchema.toString())) {
                throw new ValidationException("Cyclic schemas are not supported.");
            }
            cycleContext.seenSchemas.add(referredSchema.toString());
            toTranslate = referredSchema;
        } else {
            toTranslate = schema;
        }
        final LogicalType result = toFlinkSchema(toTranslate, isOptional, cycleContext);
        if (schema instanceof ReferenceSchema) {
            cycleContext.seenSchemas.remove(
                    ((ReferenceSchema) schema).getReferredSchema().toString());
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
                        return fromInt32Type(isOptional, schema);
                    case CONNECT_TYPE_INT64:
                        return fromInt64Type(isOptional, schema);
                    case CONNECT_TYPE_FLOAT32:
                        return new FloatType(isOptional);
                    case CONNECT_TYPE_FLOAT64:
                        return new DoubleType(isOptional);
                    case CONNECT_TYPE_BYTES: // decimal logical type
                        if (!CONNECT_TYPE_DECIMAL.equals(title)) {
                            throw new ValidationException("Expected decimal type");
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
                        throw new ValidationException("Unsupported type " + type);
                }
            }
        } else if (schema instanceof StringSchema) {
            return convertStringSchema((StringSchema) schema, isOptional);
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
                throw new ValidationException("Unsupported criterion " + criterion);
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

            // For oneOf we use the order in which the schemas are provided in the oneOf
            // declaration
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
            org.everit.json.schema.Schema allItemSchema = arraySchema.getAllItemSchema();
            if (allItemSchema == null) {
                if (arraySchema.getItemSchemas() != null) {
                    throw new ValidationException(
                            "JSON tuples (i.e. arrays that contain items of different types) are not supported");
                }
                throw new ValidationException("Array schema did not specify the items type");
            }
            String type = (String) arraySchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_MAP.equals(type) && allItemSchema instanceof ObjectSchema) {
                ObjectSchema objectSchema = (ObjectSchema) allItemSchema;
                final boolean isMultiset =
                        Objects.equals(
                                FLINK_TYPE_MULTISET,
                                arraySchema.getUnprocessedProperties().get(FLINK_TYPE_PROP));
                final LogicalType keyType =
                        toFlinkSchemaWithCycleDetection(
                                objectSchema.getPropertySchemas().get(KEY_FIELD),
                                false,
                                cycleContext);
                final LogicalType valueType =
                        toFlinkSchemaWithCycleDetection(
                                objectSchema.getPropertySchemas().get(VALUE_FIELD),
                                false,
                                cycleContext);
                return createMapLikeType(isOptional, keyType, valueType, isMultiset);
            } else {
                return new ArrayType(
                        isOptional,
                        toFlinkSchemaWithCycleDetection(allItemSchema, false, cycleContext));
            }
        } else if (schema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) schema;
            String type = (String) objectSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_MAP.equals(type)) {
                final boolean isMultiset =
                        Objects.equals(
                                FLINK_TYPE_MULTISET,
                                objectSchema.getUnprocessedProperties().get(FLINK_TYPE_PROP));
                final LogicalType valueType =
                        toFlinkSchemaWithCycleDetection(
                                objectSchema.getSchemaOfAdditionalProperties(),
                                false,
                                cycleContext);
                final VarCharType keyType = new VarCharType(false, VarCharType.MAX_LENGTH);
                return createMapLikeType(isOptional, keyType, valueType, isMultiset);
            } else {
                return convertRowType(isOptional, cycleContext, objectSchema);
            }
        } else if (schema instanceof ReferenceSchema) {
            return toFlinkSchemaWithCycleDetection(
                    ((ReferenceSchema) schema).getReferredSchema(), isOptional, cycleContext);
        } else {
            throw new ValidationException(
                    "Unsupported JSON schema type " + schema.getClass().getName());
        }
    }

    private static LogicalType createMapLikeType(
            boolean isOptional,
            LogicalType keyType,
            LogicalType valueType,
            boolean isMultisetType) {
        if (isMultisetType) {
            if (!valueType.is(LogicalTypeRoot.INTEGER)) {
                throw new ValidationException(
                        "Unexpected value type for a MULTISET type: " + valueType);
            }
            return new MultisetType(isOptional, keyType);
        } else {
            return new MapType(isOptional, keyType, valueType);
        }
    }

    private static RowType convertRowType(
            boolean isOptional, CycleContext cycleContext, ObjectSchema objectSchema) {
        Map<String, Schema> properties = objectSchema.getPropertySchemas();
        // we want to use a deterministic, explainable order, first we sort based on
        // the connect.index property, if not available we sort alphabetically. First we put
        // properties with index defined. Properties without an index are put last.
        // see resources/schema/json/mixed-indexing.json
        final Comparator<Entry<String, Schema>> indexComparator =
                Comparator.comparing(
                        e -> {
                            final Object index =
                                    e.getValue().getUnprocessedProperties().get(CONNECT_INDEX_PROP);
                            return index != null ? (Integer) index : null;
                        },
                        Comparator.nullsLast(Comparator.comparing(Function.identity())));
        final Stream<Entry<String, Schema>> sortedFields =
                properties.entrySet().stream().sorted(indexComparator.thenComparing(Entry::getKey));
        final List<RowField> rowFields =
                sortedFields
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
                                                    subSchema, isFieldOptional, cycleContext),
                                            subSchema.getDescription());
                                })
                        .collect(Collectors.toList());
        return new RowType(isOptional, rowFields);
    }

    private static LogicalType convertStringSchema(StringSchema schema, boolean isOptional) {
        final Map<String, Object> props = schema.getUnprocessedProperties();
        String type = (String) props.get(CONNECT_TYPE_PROP);
        if (CONNECT_TYPE_BYTES.equals(type)) {
            final Integer minLength =
                    (Integer) props.getOrDefault(CommonConstants.FLINK_MIN_LENGTH, null);
            final Integer maxLength =
                    (Integer) props.getOrDefault(CommonConstants.FLINK_MAX_LENGTH, null);
            if (minLength != null && Objects.equals(minLength, maxLength)) {
                return new BinaryType(isOptional, maxLength);
            } else if (maxLength != null) {
                return new VarBinaryType(isOptional, maxLength);
            } else {
                return new VarBinaryType(isOptional, VarCharType.MAX_LENGTH);
            }
        } else {
            final Integer minLength = schema.getMinLength();
            final Integer maxLength = schema.getMaxLength();
            if (minLength != null && Objects.equals(minLength, maxLength)) {
                return new CharType(isOptional, maxLength);
            } else if (maxLength != null) {
                return new VarCharType(isOptional, maxLength);
            } else {
                return new VarCharType(isOptional, VarCharType.MAX_LENGTH);
            }
        }
    }

    private static LogicalType fromInt64Type(boolean isOptional, Schema schema) {
        if (Objects.equals(schema.getTitle(), CONNECT_TYPE_TIMESTAMP)) {
            final int precision = getTimestampPrecision(schema);
            return new LocalZonedTimestampType(isOptional, precision);
        } else if (CommonConstants.FLINK_TYPE_TIMESTAMP.equals(
                schema.getUnprocessedProperties()
                        .getOrDefault(CommonConstants.FLINK_TYPE_PROP, null))) {
            final int precision = getTimestampPrecision(schema);
            return new TimestampType(isOptional, precision);
        }
        return new BigIntType(isOptional);
    }

    @SuppressWarnings("unchecked")
    private static int getTimestampPrecision(Schema schema) {
        return (int) schema.getUnprocessedProperties().getOrDefault(FLINK_PRECISION, 3);
    }

    private static LogicalType fromInt32Type(boolean isOptional, Schema schema) {
        final String title = schema.getTitle();
        if (Objects.equals(title, CONNECT_TYPE_TIME)) {
            final int precision = getTimestampPrecision(schema);
            return new TimeType(isOptional, precision);
        } else if (Objects.equals(title, CONNECT_TYPE_DATE)) {
            return new DateType(isOptional);
        } else {
            return new IntType(isOptional);
        }
    }

    private static LogicalType fromAllOfSchema(
            CombinedSchema combinedSchema, boolean isOptional, CycleContext cycleContext) {
        return toFlinkSchemaWithCycleDetection(
                CombinedSchemaUtils.simplifyAllOfSchema(combinedSchema), isOptional, cycleContext);
    }

    private static final class CycleContext {

        private final Set<String> seenSchemas = new HashSet<>();
    }
}
