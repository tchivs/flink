/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.avro;

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
import org.apache.flink.util.StringUtils;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.LongBuilder;

import java.util.List;
import java.util.Objects;

/** A converter from {@link LogicalType} to {@link Schema}. */
@Confluent
public class FlinkToAvroSchemaConverter {

    /**
     * Converts a Flink's logical type into an Avro schema. Uses Kafka Connect annotations to store
     * types that are not natively supported by Avro.
     */
    public static org.apache.avro.Schema fromFlinkSchema(LogicalType logicalType, String rowName) {
        boolean nullable = logicalType.isNullable();
        org.apache.avro.Schema notNullSchema;
        if (Objects.requireNonNull(logicalType.getTypeRoot()) == LogicalTypeRoot.NULL) {
            return SchemaBuilder.builder().nullType();
        } else {
            notNullSchema = fromFlinkSchemaIgnoreNullable(logicalType, rowName);
        }

        return nullable ? nullableSchema(notNullSchema) : notNullSchema;
    }

    private static org.apache.avro.Schema fromFlinkSchemaIgnoreNullable(
            LogicalType logicalType, String rowName) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return SchemaBuilder.builder().booleanType();
            case TINYINT:
                {
                    Schema integer = SchemaBuilder.builder().intType();
                    integer.addProp(CommonConstants.CONNECT_TYPE_PROP, "int8");
                    return integer;
                }
            case SMALLINT:
                {
                    Schema integer = SchemaBuilder.builder().intType();
                    integer.addProp(CommonConstants.CONNECT_TYPE_PROP, "int16");
                    return integer;
                }
            case INTEGER:
                return SchemaBuilder.builder().intType();
            case BIGINT:
                return SchemaBuilder.builder().longType();
            case FLOAT:
                return SchemaBuilder.builder().floatType();
            case DOUBLE:
                return SchemaBuilder.builder().doubleType();
            case CHAR:
                {
                    CharType charType = (CharType) logicalType;
                    final Schema stringType = SchemaBuilder.builder().stringType();
                    stringType.addProp(CommonConstants.FLINK_MIN_LENGTH, charType.getLength());
                    stringType.addProp(CommonConstants.FLINK_MAX_LENGTH, charType.getLength());
                    return stringType;
                }
            case VARCHAR:
                {
                    VarCharType varCharType = (VarCharType) logicalType;
                    final Schema stringType = SchemaBuilder.builder().stringType();
                    if (varCharType.getLength() < VarCharType.MAX_LENGTH) {
                        stringType.addProp(
                                CommonConstants.FLINK_MAX_LENGTH, varCharType.getLength());
                    }
                    return stringType;
                }
            case BINARY:
            case VARBINARY:
                return convertBinary(logicalType, rowName);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertLocalTimestamp((LocalZonedTimestampType) logicalType);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertTimestamp((TimestampType) logicalType);
            case DATE:
                // use int to represents Date
                return LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
            case TIME_WITHOUT_TIME_ZONE:
                return convertTime((TimeType) logicalType);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                // store BigDecimal as byte[]
                return LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale())
                        .addToSchema(SchemaBuilder.builder().bytesType());
            case ROW:
                RowType rowType = (RowType) logicalType;
                List<String> fieldNames = rowType.getFieldNames();
                // we have to make sure the record name is different in a Schema
                SchemaBuilder.FieldAssembler<Schema> builder =
                        SchemaBuilder.builder().record(rowName).fields();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    String fieldName = fieldNames.get(i);
                    if (!validateName(fieldName)) {
                        throw getInvalidFieldNameException(fieldName);
                    }
                    LogicalType fieldType = rowType.getTypeAt(i);
                    SchemaBuilder.GenericDefault<Schema> fieldBuilder =
                            builder.name(fieldName)
                                    .type(fromFlinkSchema(fieldType, rowName + "_" + fieldName));

                    if (fieldType.isNullable()) {
                        builder = fieldBuilder.withDefault(null);
                    } else {
                        builder = fieldBuilder.noDefault();
                    }
                }
                return builder.endRecord();
            case MAP:
                return convertMap((MapType) logicalType, rowName);
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                return SchemaBuilder.builder()
                        .array()
                        .items(fromFlinkSchema(arrayType.getElementType(), rowName));
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
                        "Unsupported to derive an Avro Schema for type " + logicalType);
        }
    }

    private static ValidationException getInvalidFieldNameException(String fieldName) {
        return new ValidationException(
                String.format(
                        "Illegal field name for AVRO format "
                                + "`%s`. AVRO expects field"
                                + " names to start with [A-Za-z_] subsequently contain only [A-Za-z0-9_].",
                        fieldName));
    }

    private static Schema convertMap(MapType logicalType, String rowName) {
        final LogicalType keyType = logicalType.getKeyType();
        final LogicalType valueType = logicalType.getValueType();

        return convertMapLikeType(rowName, keyType, valueType);
    }

    private static Schema convertMultiset(MultisetType logicalType, String rowName) {
        final LogicalType keyType = logicalType.getElementType();
        final LogicalType valueType = new IntType(false);

        final Schema schema = convertMapLikeType(rowName, keyType, valueType);
        schema.addProp(CommonConstants.FLINK_TYPE, CommonConstants.FLINK_MULTISET_TYPE);
        return schema;
    }

    private static Schema convertMapLikeType(
            String rowName, LogicalType keyType, LogicalType valueType) {
        if (keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return SchemaBuilder.builder().map().values(fromFlinkSchema(valueType, rowName));
        } else {
            return connectCustomMap(rowName, keyType, valueType);
        }
    }

    private static Schema connectCustomMap(
            String rowName, LogicalType keyType, LogicalType valueType) {
        return SchemaBuilder.array()
                .items(
                        SchemaBuilder.record("MapEntry")
                                .namespace("io.confluent.connect.avro")
                                .fields()
                                .name(CommonConstants.KEY_FIELD)
                                .type(fromFlinkSchema(keyType, rowName + "_key"))
                                .noDefault()
                                .name(CommonConstants.VALUE_FIELD)
                                .type(fromFlinkSchema(valueType, rowName + "_value"))
                                .noDefault()
                                .endRecord());
    }

    private static Schema convertTime(TimeType logicalType) {
        final int precision = logicalType.getPrecision();
        if (precision == 3) {
            return LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
        } else if (precision < 3) {
            return LogicalTypes.timeMillis()
                    .addToSchema(
                            SchemaBuilder.builder()
                                    .intBuilder()
                                    .prop(CommonConstants.FLINK_PRECISION, precision)
                                    .endInt());
        } else {
            throw new ValidationException(
                    "Flink does not support TIME type with precision "
                            + precision
                            + ", it only supports precision less than or equal to 3.");
        }
    }

    private static Schema convertTimestamp(TimestampType logicalType) {
        final int precision = logicalType.getPrecision();
        final org.apache.avro.LogicalType avroLogicalType;
        if (precision <= 3) {
            avroLogicalType = LogicalTypes.localTimestampMillis();
        } else if (precision <= 6) {
            avroLogicalType = LogicalTypes.localTimestampMicros();
        } else {
            throw new ValidationException(
                    "Avro does not support TIMESTAMP type "
                            + "with precision "
                            + precision
                            + ", it only supports precision less than or equal to 6.");
        }
        LongBuilder<Schema> longBuilder = SchemaBuilder.builder().longBuilder();
        if (precision != 3 && precision != 6) {
            longBuilder = longBuilder.prop(CommonConstants.FLINK_PRECISION, precision);
        }
        return avroLogicalType.addToSchema(longBuilder.endLong());
    }

    private static Schema convertLocalTimestamp(LocalZonedTimestampType logicalType) {
        final int precision = logicalType.getPrecision();
        final org.apache.avro.LogicalType avroLogicalType;
        if (precision <= 3) {
            avroLogicalType = LogicalTypes.timestampMillis();
        } else if (precision <= 6) {
            avroLogicalType = LogicalTypes.timestampMicros();
        } else {
            throw new ValidationException(
                    "Avro does not support TIMESTAMP_LTZ type "
                            + "with precision "
                            + precision
                            + ", it only supports precision less than or equal to 6.");
        }
        LongBuilder<Schema> longBuilder = SchemaBuilder.builder().longBuilder();
        if (precision != 3 && precision != 6) {
            longBuilder = longBuilder.prop(CommonConstants.FLINK_PRECISION, precision);
        }
        return avroLogicalType.addToSchema(longBuilder.endLong());
    }

    private static Schema convertBinary(LogicalType logicalType, String rowName) {
        final int length;
        if (logicalType instanceof BinaryType) {
            length = ((BinaryType) logicalType).getLength();
        } else if (logicalType instanceof VarBinaryType) {
            length = ((VarBinaryType) logicalType).getLength();
        } else {
            throw new IllegalStateException("Unexpected logical type: " + logicalType);
        }

        // max length is the same both for BINARY and VARBINARY
        if (length == VarBinaryType.MAX_LENGTH) {
            return SchemaBuilder.builder().bytesType();
        } else {
            return SchemaBuilder.fixed(rowName).size(length);
        }
    }

    /** Returns schema with nullable true. */
    private static Schema nullableSchema(Schema schema) {
        return schema.isNullable()
                ? schema
                : Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
    }

    // Extracted from org.apache.avro.Schema.validateName
    private static boolean validateName(String name) {
        if (StringUtils.isNullOrWhitespaceOnly(name)) {
            return false;
        }
        char first = name.charAt(0);
        if (!(Character.isLetter(first) || first == '_')) {
            return false;
        }
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_')) {
                return false;
            }
        }
        return true;
    }
}
