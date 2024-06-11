/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.protobuf;

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
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.flink.shaded.guava31.com.google.common.base.CaseFormat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto.Builder;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import io.confluent.protobuf.type.Decimal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * A converter from {@link LogicalType} to {@link Descriptor}.
 *
 * <p>The mapping is represented by the following table:
 *
 * <pre>
 * +------------------------+------------------+---------------------------+-------------------------------------------+
 * |       Flink type       |  Protobuf type   |       Message type        |                  Comment                  |
 * +------------------------+------------------+---------------------------+-------------------------------------------+
 * | BOOLEAN                | BOOL             |                           |                                           |
 * | TINYINT                | INT32            | -                         | MetaProto extension: connect.type = int8  |
 * | SMALLINT               | INT32            | -                         | MetaProto extension: connect.type = int16 |
 * | INT                    | INT32            |                           |                                           |
 * | BIGINT                 | INT64            |                           |                                           |
 * | FLOAT                  | FLOAT            |                           |                                           |
 * | DOUBLE                 | DOUBLE           |                           |                                           |
 * | CHAR                   | STRING           |                           |                                           |
 * | VARCHAR                | STRING           |                           |                                           |
 * | BINARY                 | BYTES            |                           |                                           |
 * | VARBINARY              | BYTES            |                           |                                           |
 * | TIMESTAMP_LTZ          | MESSAGE          | google.protobuf.Timestamp |                                           |
 * | DATE                   | MESSAGE          | google.type.Date          |                                           |
 * | TIME_WITHOUT_TIME_ZONE | MESSAGE          | google.type.TimeOfDay     |                                           |
 * | DECIMAL                | MESSAGE          | confluent.type.Decimal    |                                           |
 * | MAP[K, V]              | repeated MESSAGE | XXEntry(K key, V value)   |                                           |
 * | ARRAY[T]               | repeated T       |                           |                                           |
 * | ROW                    | MESSAGE          | fieldName                 |                                           |
 * +------------------------+------------------+---------------------------+-------------------------------------------+
 * </pre>
 *
 * <p>When converting to a Protobuf schema we mark all NULLABLE fields as optional.
 */
@Confluent
public class FlinkToProtoSchemaConverter {

    /**
     * Flink {@link LogicalType logicalTypes} that map to protobufs {@link Type#TYPE_MESSAGE}, which
     * needs special annotation for marking NOT NULL types.
     */
    private static final Set<LogicalTypeRoot> PROTO_MESSAGE_TYPES =
            EnumSet.of(
                    LogicalTypeRoot.DECIMAL,
                    LogicalTypeRoot.DATE,
                    LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                    LogicalTypeRoot.ROW);

    /**
     * Converts a Flink's logical type into a Protobuf descriptor. Uses Kafka Connect logic to store
     * types that are not natively supported.
     */
    public static Descriptor fromFlinkSchema(
            RowType logicalType, String rowName, String packageName) {
        try {
            final Set<String> dependencies = new TreeSet<>();
            final DescriptorProto builder = fromRowType(logicalType, rowName, dependencies);
            final FileDescriptorProto fileDescriptorProto =
                    FileDescriptorProto.newBuilder()
                            .addMessageType(builder)
                            .setPackage(packageName)
                            .setSyntax("proto3")
                            .addAllDependency(dependencies)
                            .build();
            return FileDescriptor.buildFrom(
                            fileDescriptorProto,
                            Stream.of(
                                            Meta.getDescriptor(),
                                            Date.getDescriptor(),
                                            TimeOfDay.getDescriptor(),
                                            Timestamp.getDescriptor(),
                                            Decimal.getDescriptor())
                                    .map(Descriptor::getFile)
                                    .toArray(FileDescriptor[]::new))
                    .getFile()
                    .findMessageTypeByName(rowName);
        } catch (DescriptorValidationException e) {
            throw new ValidationException(
                    "Failed to translate the provided schema to a Protobuf descriptor", e);
        }
    }

    private static DescriptorProto fromRowType(
            RowType logicalType, String rowName, Set<String> dependencies) {
        final Builder builder = DescriptorProto.newBuilder();

        builder.setName(rowName);
        final List<DescriptorProto> nestedRows = new ArrayList<>();
        final List<RowField> fields = logicalType.getFields();
        for (int i = 0; i < logicalType.getFieldCount(); i++) {
            final RowField field = fields.get(i);
            builder.addField(
                    fromRowField(
                            field.getType(), field.getName(), i + 1, nestedRows, dependencies));
        }
        builder.addAllNestedType(nestedRows);
        return builder.build();
    }

    private static FieldDescriptorProto.Builder fromRowField(
            LogicalType logicalType,
            String fieldName,
            int fieldIndex,
            List<DescriptorProto> nestedRows,
            Set<String> dependencies) {
        final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
        builder.setName(fieldName);
        builder.setNumber(fieldIndex);
        if (!logicalType.isNullable()) {
            builder.setLabel(Label.LABEL_REQUIRED);
        } else {
            builder.setProto3Optional(logicalType.isNullable());
        }

        if (!logicalType.isNullable() && PROTO_MESSAGE_TYPES.contains(logicalType.getTypeRoot())) {
            addMetaParam(builder, CommonConstants.FLINK_NOT_NULL, "true");
        }

        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                builder.setType(Type.TYPE_BOOL);
                return builder;
            case TINYINT:
                builder.setType(Type.TYPE_INT32);
                addMetaParam(
                        builder,
                        CommonConstants.CONNECT_TYPE_PROP,
                        CommonConstants.CONNECT_TYPE_INT8);
                return builder;
            case SMALLINT:
                builder.setType(Type.TYPE_INT32);
                addMetaParam(
                        builder,
                        CommonConstants.CONNECT_TYPE_PROP,
                        CommonConstants.CONNECT_TYPE_INT16);
                return builder;
            case INTEGER:
                builder.setType(Type.TYPE_INT32);
                return builder;
            case BIGINT:
                builder.setType(Type.TYPE_INT64);
                return builder;
            case FLOAT:
                builder.setType(Type.TYPE_FLOAT);
                return builder;
            case DOUBLE:
                builder.setType(Type.TYPE_DOUBLE);
                return builder;
            case CHAR:
                return createLengthLimitedType(
                        builder,
                        Type.TYPE_STRING,
                        ((CharType) logicalType).getLength(),
                        ((CharType) logicalType).getLength());
            case VARCHAR:
                return createLengthLimitedType(
                        builder, Type.TYPE_STRING, -1, ((VarCharType) logicalType).getLength());
            case BINARY:
                return createLengthLimitedType(
                        builder,
                        Type.TYPE_BYTES,
                        ((BinaryType) logicalType).getLength(),
                        ((BinaryType) logicalType).getLength());
            case VARBINARY:
                return createLengthLimitedType(
                        builder, Type.TYPE_BYTES, -1, ((VarBinaryType) logicalType).getLength());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimestampFieldDescriptor(
                        ((TimestampType) logicalType).getPrecision(),
                        logicalType.getTypeRoot(),
                        dependencies,
                        builder);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return createTimestampFieldDescriptor(
                        ((LocalZonedTimestampType) logicalType).getPrecision(),
                        logicalType.getTypeRoot(),
                        dependencies,
                        builder);
            case DATE:
                builder.setType(Type.TYPE_MESSAGE);
                builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_DATE_TYPE));
                dependencies.add(CommonConstants.PROTOBUF_DATE_LOCATION);
                return builder;
            case TIME_WITHOUT_TIME_ZONE:
                return createTimeFieldDescriptor(
                        ((TimeType) logicalType).getPrecision(), dependencies, builder);
            case DECIMAL:
                return createDecimalField((DecimalType) logicalType, dependencies, builder);
            case ROW:
                return createMessageField(
                        (RowType) logicalType, fieldName, nestedRows, dependencies, builder);
            case MAP:
                final MapType mapType = (MapType) logicalType;
                if (mapType.isNullable()) {
                    return wrapRepeatedType(
                            mapType, fieldName, fieldIndex, nestedRows, dependencies);
                } else {
                    return createNotNullMapLikeField(
                            fieldName,
                            fieldIndex,
                            nestedRows,
                            dependencies,
                            mapType.getKeyType(),
                            mapType.getValueType());
                }
            case ARRAY:
                if (logicalType.isNullable()) {
                    return wrapRepeatedType(
                            logicalType, fieldName, fieldIndex, nestedRows, dependencies);
                } else {
                    return createRepeatedNotNull(
                            fieldName,
                            fieldIndex,
                            nestedRows,
                            dependencies,
                            ((ArrayType) logicalType).getElementType());
                }
            case MULTISET:
                return createMultisetType(
                        (MultisetType) logicalType,
                        fieldName,
                        fieldIndex,
                        nestedRows,
                        dependencies);
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
                        "Unsupported to derive Protobuf Schema for type " + logicalType);
        }
    }

    private static FieldDescriptorProto.Builder createMultisetType(
            MultisetType logicalType,
            String fieldName,
            int fieldIndex,
            List<DescriptorProto> nestedRows,
            Set<String> dependencies) {
        final MultisetType multisetType = logicalType;
        final FieldDescriptorProto.Builder builder;
        if (multisetType.isNullable()) {
            builder =
                    wrapRepeatedType(multisetType, fieldName, fieldIndex, nestedRows, dependencies);
        } else {
            builder =
                    createNotNullMapLikeField(
                            fieldName,
                            fieldIndex,
                            nestedRows,
                            dependencies,
                            multisetType.getElementType(),
                            new IntType(false));
        }
        addMetaParam(builder, CommonConstants.FLINK_TYPE_PROP, CommonConstants.FLINK_TYPE_MULTISET);
        return builder;
    }

    private static FieldDescriptorProto.Builder createDecimalField(
            DecimalType decimalType,
            Set<String> dependencies,
            FieldDescriptorProto.Builder builder) {
        builder.setType(Type.TYPE_MESSAGE);
        final Map<String, String> params = new HashMap<>();
        params.put(
                CommonConstants.PROTOBUF_PRECISION_PROP,
                String.valueOf(decimalType.getPrecision()));
        params.put(CommonConstants.PROTOBUF_SCALE_PROP, String.valueOf(decimalType.getScale()));
        addMetaParams(builder, params);

        builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_DECIMAL_TYPE));
        dependencies.add(CommonConstants.PROTOBUF_DECIMAL_LOCATION);
        return builder;
    }

    private static FieldDescriptorProto.Builder createMessageField(
            RowType logicalType,
            String fieldName,
            List<DescriptorProto> nestedRows,
            Set<String> dependencies,
            FieldDescriptorProto.Builder builder) {
        // field name uniqueness should suffice for type naming. Each type is scoped to
        // the enclosing Row. If a Row is nested within a nested Row, those two won't
        // have collisions. Thus it is possible to have:
        // message A {
        //  b_Row b
        //  message b_Row {
        //    b_Row b
        //    message b_Row {
        //      int32 c;
        //    }
        //  }
        // }
        final String typeName = fieldName + "_Row";
        final DescriptorProto nestedRowDescriptor =
                fromRowType(logicalType, typeName, dependencies);
        nestedRows.add(nestedRowDescriptor);
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(typeName);
        return builder;
    }

    private static FieldDescriptorProto.Builder createLengthLimitedType(
            FieldDescriptorProto.Builder builder, Type type, int minLength, int maxLength) {
        final Map<String, String> params = new HashMap<>();
        if (minLength == maxLength && minLength > 0) {
            // CHAR or BINARY case
            params.put(CommonConstants.FLINK_MIN_LENGTH, String.valueOf(minLength));
            params.put(CommonConstants.FLINK_MAX_LENGTH, String.valueOf(maxLength));
        } else if (maxLength != VarCharType.MAX_LENGTH) {
            params.put(CommonConstants.FLINK_MAX_LENGTH, String.valueOf(maxLength));
        }

        if (!params.isEmpty()) {
            addMetaParams(builder, params);
        }

        builder.setType(type);
        return builder;
    }

    private static FieldDescriptorProto.Builder createTimeFieldDescriptor(
            int precision, Set<String> dependencies, FieldDescriptorProto.Builder builder) {
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_TIME_TYPE));
        if (precision != 9) {
            addMetaParam(builder, CommonConstants.FLINK_PRECISION_PROP, String.valueOf(precision));
        }
        dependencies.add(CommonConstants.PROTOBUF_TIME_LOCATION);
        return builder;
    }

    private static FieldDescriptorProto.Builder createTimestampFieldDescriptor(
            int precision,
            LogicalTypeRoot typeRoot,
            Set<String> dependencies,
            FieldDescriptorProto.Builder builder) {
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_TIMESTAMP_TYPE));
        final Map<String, String> params = new HashMap<>();
        if (precision != 9) {
            params.put(CommonConstants.FLINK_PRECISION_PROP, String.valueOf(precision));
        }
        if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            params.put(CommonConstants.FLINK_TYPE_PROP, CommonConstants.FLINK_TYPE_TIMESTAMP);
        }
        addMetaParams(builder, params);
        dependencies.add(CommonConstants.PROTOBUF_TIMESTAMP_LOCATION);
        return builder;
    }

    private static String makeItTopLevelScoped(String type) {
        // we scope types to the top level by prepending them with a dot. otherwise protobuf looks
        // for the types in the current scope. This makes it especially difficult if the current
        // scope has a common prefix with the given type e.g. io.confluent.generated.Row and
        // io.confluent.type.Decimal.
        return "." + type;
    }

    private static FieldDescriptorProto.Builder wrapRepeatedType(
            LogicalType repeatedType,
            String fieldName,
            int fieldIndex,
            List<DescriptorProto> nestedRows,
            Set<String> dependencies) {
        final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
        // Repeated keyword can not nest collections nor nullable types.
        // Wrap into an artificial message with a Flink specific annotation.
        final String typeName = fieldName + CommonConstants.FLINK_REPEATED_WRAPPER_SUFFIX;

        nestedRows.add(
                fromRowType(
                        new RowType(
                                false,
                                Collections.singletonList(
                                        new RowField(
                                                CommonConstants.FLINK_WRAPPER_FIELD_NAME,
                                                repeatedType.copy(false)))),
                        typeName,
                        dependencies));
        addMetaParam(builder, CommonConstants.FLINK_WRAPPER, "true");
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(typeName);
        builder.setNumber(fieldIndex);
        builder.setName(fieldName);
        return builder;
    }

    private static FieldDescriptorProto.Builder createRepeatedNotNull(
            String fieldName,
            int fieldIndex,
            List<DescriptorProto> nestedRows,
            Set<String> dependencies,
            LogicalType elementType) {
        final FieldDescriptorProto.Builder builder;
        if (elementType.is(LogicalTypeFamily.COLLECTION)
                || elementType.is(LogicalTypeRoot.MAP)
                || elementType.isNullable()) {
            // Repeated keyword can not nest collections nor nullable types.
            // Wrap into an artificial message with a Flink specific annotation.
            final String typeName = fieldName + CommonConstants.FLINK_ELEMENT_WRAPPER_SUFFIX;

            nestedRows.add(
                    fromRowType(
                            new RowType(
                                    false,
                                    Collections.singletonList(
                                            new RowField(
                                                    CommonConstants.FLINK_WRAPPER_FIELD_NAME,
                                                    elementType))),
                            typeName,
                            dependencies));
            builder = FieldDescriptorProto.newBuilder();
            addMetaParam(builder, CommonConstants.FLINK_WRAPPER, "true");
            builder.setType(Type.TYPE_MESSAGE);
            builder.setTypeName(typeName);
            builder.setNumber(fieldIndex);
            builder.setName(fieldName);
        } else {
            builder = fromRowField(elementType, fieldName, fieldIndex, nestedRows, dependencies);
        }

        builder.setLabel(Label.LABEL_REPEATED);
        // repeated fields can not be optional
        builder.clearProto3Optional();
        return builder;
    }

    private static void addMetaParam(
            FieldDescriptorProto.Builder builder, String paramKey, String paramValue) {
        addMetaParams(builder, Collections.singletonMap(paramKey, paramValue));
    }

    private static void addMetaParams(
            FieldDescriptorProto.Builder builder, Map<String, String> params) {
        builder.mergeOptions(
                FieldOptions.newBuilder()
                        .setExtension(
                                MetaProto.fieldMeta, Meta.newBuilder().putAllParams(params).build())
                        .build());
    }

    private static FieldDescriptorProto.Builder createNotNullMapLikeField(
            String fieldName,
            int fieldIndex,
            List<DescriptorProto> nestedRows,
            Set<String> dependencies,
            LogicalType keyType,
            LogicalType valueType) {

        final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
        // Protobuf does not have a native support for a MAP type. It does represent a syntactic
        // sugar such as:
        //  message A { map<string, int32> map_field } is equivalent to:
        //  message A { repeated MapFieldEntry map_field message MapFieldEntry { string key, int32
        // value}}
        // we keep the naming strategy compatible here
        final String typeName =
                CaseFormat.LOWER_UNDERSCORE.to(
                        CaseFormat.UPPER_CAMEL, fieldName + "_" + CommonConstants.MAP_ENTRY_SUFFIX);
        final DescriptorProto mapDescriptor =
                fromRowType(
                        new RowType(
                                false, // repeated element can not be null
                                Arrays.asList(
                                        new RowField(CommonConstants.KEY_FIELD, keyType),
                                        new RowField(CommonConstants.VALUE_FIELD, valueType))),
                        typeName,
                        dependencies);
        nestedRows.add(mapDescriptor);
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(typeName);
        builder.setNumber(fieldIndex);
        builder.setLabel(Label.LABEL_REPEATED);
        builder.setName(fieldName);
        builder.clearProto3Optional();
        return builder;
    }
}
