/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ObjectSchema.Builder;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_TIME;
import static io.confluent.flink.formats.converters.json.CommonConstants.CONNECT_TYPE_TIMESTAMP;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_PARAMETERS;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_PRECISION;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_PROP;
import static io.confluent.flink.formats.converters.json.CommonConstants.FLINK_TYPE_TIMESTAMP;

/**
 * A converter from {@link LogicalType} to {@link Schema}.
 *
 * <pre>
 * +-------------------+---------------------------+-------------------------+-----------------------------------------+
 * |    Flink type     |         Json type         | Connect type annotation |             Json type title             |
 * +-------------------+---------------------------+-------------------------+-----------------------------------------+
 * | BOOLEAN           | BooleanSchema             |                         |                                         |
 * | TINYINT           | NumberSchema              | int8                    |                                         |
 * | SMALLINT          | NumberSchema              | int16                   |                                         |
 * | INT               | NumberSchema              | int32                   |                                         |
 * | BIGINT            | NumberSchema              | int64                   |                                         |
 * | FLOAT             | NumberSchema              | float32                 |                                         |
 * | DOUBLE            | NumberSchema              | float64                 |                                         |
 * | CHAR              | StringSchema              |                         |                                         |
 * | VARCHAR           | StringSchema              |                         |                                         |
 * | BINARY            | StringSchema              | bytes                   |                                         |
 * | VARBINARY         | StringSchema              | bytes                   |                                         |
 * | TIMESTAMP_LTZ     | NumberSchema              | int64                   | org.apache.kafka.connect.data.Timestamp |
 * | DATE              | NumberSchema              | int32                   | org.apache.kafka.connect.data.Date      |
 * | TIME              | NumberSchema              | int32                   | org.apache.kafka.connect.data.Time      |
 * | DECIMAL           | NumberSchema              | bytes                   | org.apache.kafka.connect.data.Decimal   |
 * | ROW               | ObjectSchema              |                         |                                         |
 * | MAP[VARCHAR, V]   | ObjectSchema              | map                     |                                         |
 * | MAP[K, V]         | ArraySchema[ObjectSchema] | map                     |                                         |
 * | MULTISET[VARCHAR] | ObjectSchema              | map                     |                                         |
 * | MULTISET[K]       | ArraySchema[ObjectSchema] | map                     |                                         |
 * | ARRAY             | ArraySchema               |                         |                                         |
 * +-------------------+---------------------------+-------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>nullable types are expressed as oneOf(NullSchema, T)
 *   <li>ObjectSchema for a MAP and MULTISET must have two fields [key, value]
 *   <li>MULTISET is equivalent to MAP[K, INT] and is serialised accordingly
 * </ul>
 */
@Confluent
public class FlinkToJsonSchemaConverter {

    /**
     * Converts a Flink's logical type into an Avro schema. Uses Kafka Connect annotations to store
     * types that are not natively supported by Avro.
     */
    public static Schema fromFlinkSchema(LogicalType logicalType, String rowName) {
        return fromFlinkSchemaBuilder(logicalType, rowName).build();
    }

    private static Schema.Builder<?> fromFlinkSchemaBuilder(
            LogicalType logicalType, String rowName) {
        boolean nullable = logicalType.isNullable();
        Schema.Builder<?> notNullSchema;
        if (Objects.requireNonNull(logicalType.getTypeRoot()) == LogicalTypeRoot.NULL) {
            return NullSchema.builder();
        } else {
            notNullSchema = fromFlinkSchemaIgnoreNullable(logicalType, rowName);
        }

        return nullable ? nullableSchema(notNullSchema.build()) : notNullSchema;
    }

    private static Schema.Builder<?> fromFlinkSchemaIgnoreNullable(
            LogicalType logicalType, String rowName) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return BooleanSchema.builder();
            case TINYINT:
                return NumberSchema.builder()
                        .unprocessedProperties(
                                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT8));
            case SMALLINT:
                return NumberSchema.builder()
                        .unprocessedProperties(
                                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT16));
            case INTEGER:
                return NumberSchema.builder()
                        .unprocessedProperties(
                                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT32));
            case BIGINT:
                return NumberSchema.builder()
                        .unprocessedProperties(
                                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64));
            case FLOAT:
                return NumberSchema.builder()
                        .unprocessedProperties(
                                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_FLOAT32));
            case DOUBLE:
                return NumberSchema.builder()
                        .unprocessedProperties(
                                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_FLOAT64));
            case CHAR:
                {
                    CharType charType = (CharType) logicalType;
                    final StringSchema.Builder builder = StringSchema.builder();
                    builder.minLength(charType.getLength());
                    builder.maxLength(charType.getLength());
                    return builder;
                }
            case VARCHAR:
                {
                    VarCharType varcharType = (VarCharType) logicalType;
                    final StringSchema.Builder builder = StringSchema.builder();
                    if (varcharType.getLength() < VarCharType.MAX_LENGTH) {
                        builder.maxLength(varcharType.getLength());
                    }
                    return builder;
                }
            case BINARY:
                BinaryType binaryType = (BinaryType) logicalType;
                return createBinaryStringType(binaryType.getLength(), binaryType.getLength());
            case VARBINARY:
                VarBinaryType varBinaryType = (VarBinaryType) logicalType;
                return createBinaryStringType(-1, varBinaryType.getLength());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertTimestamp(
                        ((TimestampType) logicalType).getPrecision(), logicalType.getTypeRoot());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTimestamp(
                        ((LocalZonedTimestampType) logicalType).getPrecision(),
                        logicalType.getTypeRoot());
            case DATE:
                // use int to represents Date
                return NumberSchema.builder()
                        .title(CONNECT_TYPE_DATE)
                        .unprocessedProperties(
                                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT32));
            case TIME_WITHOUT_TIME_ZONE:
                return convertTime((TimeType) logicalType);
            case DECIMAL:
                final Map<String, Object> props = getDecimalProperties((DecimalType) logicalType);

                return NumberSchema.builder()
                        .unprocessedProperties(props)
                        .title(CONNECT_TYPE_DECIMAL);
            case ROW:
                RowType rowType = (RowType) logicalType;
                List<String> fieldNames = rowType.getFieldNames();
                // we have to make sure the record name is different in a Schema
                final Builder rowBuilder = ObjectSchema.builder();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    String fieldName = fieldNames.get(i);
                    LogicalType fieldType = rowType.getTypeAt(i);
                    final Schema.Builder<?> fieldSchema =
                            fromFlinkSchemaBuilder(fieldType, rowName + "_" + fieldName);
                    final Map<String, Object> extendedProps =
                            new HashMap<>(fieldSchema.unprocessedProperties);
                    extendedProps.put(CONNECT_INDEX_PROP, i);
                    fieldSchema.unprocessedProperties(extendedProps);
                    rowBuilder.addPropertySchema(fieldName, fieldSchema.build());

                    if (!fieldType.isNullable()) {
                        rowBuilder.addRequiredProperty(fieldName);
                    }
                }
                return rowBuilder.title(rowName);
            case MAP:
                return convertMap((MapType) logicalType, rowName);
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                return ArraySchema.builder()
                        .allItemSchema(fromFlinkSchema(arrayType.getElementType(), rowName));
            case MULTISET:
                return convertMultiset((MultisetType) logicalType, rowName);
            case TIMESTAMP_WITH_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case SYMBOL:
            case UNRESOLVED:
            case RAW:
            default:
                throw new ValidationException(
                        "Unsupported to derive JSON Schema for type: " + logicalType);
        }
    }

    private static Schema.Builder<StringSchema> createBinaryStringType(
            int minLength, int maxLength) {
        final StringSchema.Builder builder = StringSchema.builder();
        final Map<String, Object> props = new HashMap<>();
        if (minLength == maxLength && minLength > 0) {
            // BINARY case
            props.put(CommonConstants.FLINK_MIN_LENGTH, minLength);
            props.put(CommonConstants.FLINK_MAX_LENGTH, maxLength);
        } else if (maxLength < BinaryType.MAX_LENGTH) {
            props.put(CommonConstants.FLINK_MAX_LENGTH, maxLength);
        }
        props.put(CONNECT_TYPE_PROP, CONNECT_TYPE_BYTES);
        return builder.unprocessedProperties(props);
    }

    private static Map<String, Object> getDecimalProperties(DecimalType logicalType) {
        final Map<String, Object> props = new HashMap<>();
        props.put(CONNECT_TYPE_PROP, CONNECT_TYPE_BYTES);

        final Map<String, Object> parameters = new HashMap<>();
        parameters.put(CONNECT_TYPE_DECIMAL_PRECISION, String.valueOf(logicalType.getPrecision()));
        parameters.put(CONNECT_TYPE_DECIMAL_SCALE, String.valueOf(logicalType.getScale()));
        props.put(CONNECT_PARAMETERS, parameters);
        return props;
    }

    private static Schema.Builder<?> convertMap(MapType logicalType, String rowName) {
        final LogicalType keyType = logicalType.getKeyType();
        final LogicalType valueType = logicalType.getValueType();

        return convertMapLikeType(rowName, keyType, valueType);
    }

    private static Schema.Builder<?> convertMultiset(MultisetType logicalType, String rowName) {
        final LogicalType keyType = logicalType.getElementType();
        final LogicalType valueType = new IntType(false);

        return convertMapLikeType(rowName, keyType, valueType);
    }

    private static Schema.Builder<?> convertMapLikeType(
            String rowName, LogicalType keyType, LogicalType valueType) {
        if (keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return ObjectSchema.builder()
                    .schemaOfAdditionalProperties(fromFlinkSchema(valueType, rowName))
                    .unprocessedProperties(Collections.singletonMap("connect.type", "map"));
        } else {
            return connectCustomMap(rowName, keyType, valueType);
        }
    }

    private static Schema.Builder<?> connectCustomMap(
            String rowName, LogicalType keyType, LogicalType valueType) {
        return ArraySchema.builder()
                .allItemSchema(
                        ObjectSchema.builder()
                                .addPropertySchema(
                                        "key", fromFlinkSchema(keyType, rowName + "_key"))
                                .addPropertySchema(
                                        "value", fromFlinkSchema(valueType, rowName + "_value"))
                                .build())
                .unprocessedProperties(Collections.singletonMap("connect.type", "map"));
    }

    private static Schema.Builder<?> convertTime(TimeType logicalType) {
        final int precision = logicalType.getPrecision();
        if (precision <= 3) {
            final Map<String, Object> properties = new HashMap<>();
            properties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT32);
            if (precision != 3) {
                properties.put(
                        FLINK_PARAMETERS, Collections.singletonMap(FLINK_PRECISION, precision));
            }
            return NumberSchema.builder()
                    .title(CONNECT_TYPE_TIME)
                    .unprocessedProperties(properties);
        } else {
            throw new ValidationException(
                    "Flink does not support TIME type with precision: "
                            + precision
                            + ", it only supports precision less than or equal to 3.");
        }
    }

    private static Schema.Builder<?> convertTimestamp(int precision, LogicalTypeRoot typeRoot) {
        if (precision <= 3) {
            final Map<String, Object> properties = new HashMap<>();
            properties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64);
            if (precision != 3) {
                properties.put(
                        FLINK_PARAMETERS, Collections.singletonMap(FLINK_PRECISION, precision));
            }
            Schema.Builder<NumberSchema> builder = NumberSchema.builder();
            if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                builder = builder.title(CONNECT_TYPE_TIMESTAMP);
            } else {
                properties.put(FLINK_TYPE_PROP, FLINK_TYPE_TIMESTAMP);
            }
            return builder.unprocessedProperties(properties);
        } else {
            throw new ValidationException(
                    String.format(
                            "JSON does not support %s type "
                                    + "with precision "
                                    + precision
                                    + ", it only supports precision less than or equal to 3.",
                            typeRoot));
        }
    }

    /** Returns schema with nullable true. */
    private static Schema.Builder<CombinedSchema> nullableSchema(Schema schema) {
        return CombinedSchema.builder()
                .criterion(CombinedSchema.ONE_CRITERION)
                .subschema(NullSchema.INSTANCE)
                .subschema(schema);
    }
}
