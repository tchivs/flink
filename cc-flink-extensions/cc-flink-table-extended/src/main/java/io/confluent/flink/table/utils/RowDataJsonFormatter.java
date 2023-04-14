/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ArrayData.ElementGetter;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.planner.functions.casting.CastRule.Context;
import org.apache.flink.table.planner.functions.casting.CastRuleProvider;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.ZoneId;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A JSON formatter for serving {@link RowData} results.
 *
 * <p>It uses SQL for atomic values, and JSON for wrappers and NULL. We represent atomic values as
 * SQL strings, all nested wrappers as arrays, and NULL as JSON null.
 *
 * <p><b>Example</b>
 *
 * <pre>
 * // Example for INT, STRING, ROW&lt;...&gt;, ARRAY, TIMESTAMP, MAP&lt;INT, STRING&gt; in SQL:
 * {"op":0,"row":["101","Jay",[null,"abc"],[null,"456"],"1990-01-12 12:00.12",[[null,"Alice"],["42","Bob"]]]}
 * </pre>
 */
@Internal
public final class RowDataJsonFormatter {
    private final Context castRuleContext;
    private final Converter rowConverter;

    private final ObjectMapper objectMapper;

    private final ObjectNode topNode;

    private final boolean isInsertOnly;

    public RowDataJsonFormatter(
            LogicalType consumedType,
            ZoneId zoneId,
            ClassLoader classLoader,
            boolean isInsertOnly) {
        this.castRuleContext = Context.create(true, false, zoneId, classLoader);
        this.rowConverter = createConverter(consumedType);
        this.objectMapper = new ObjectMapper();
        this.topNode = objectMapper.getNodeFactory().objectNode();
        this.isInsertOnly = isInsertOnly;
    }

    /** Converts the incoming data into a JSON-y format. */
    public String convert(RowData rowData) {
        final JsonNode row = rowConverter.apply(rowData, objectMapper);
        if (!isInsertOnly) {
            topNode.set(
                    "op",
                    objectMapper.getNodeFactory().numberNode(rowData.getRowKind().toByteValue()));
        }
        topNode.set("row", row);
        return topNode.toString();
    }

    private interface Converter {
        JsonNode apply(Object rowData, ObjectMapper objectMapper);
    }

    private Converter createConverter(LogicalType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case ARRAY:
                return createArrayConverter(fieldType.getChildren().get(0));
            case MULTISET:
                return createMapConverter(fieldType.getChildren().get(0), new IntType(false));
            case MAP:
                return createMapConverter(
                        fieldType.getChildren().get(0), fieldType.getChildren().get(1));
            case ROW:
                return convertRowConverter(fieldType.getChildren());
            case NULL:
                return (rowData, mapper) -> mapper.nullNode();
            default:
                return convertAtomicFieldConverter(fieldType);
        }
    }

    private Converter createArrayConverter(LogicalType elementType) {
        final Converter elementConverter = createConverter(elementType);
        final ElementGetter elementGetter = ArrayData.createElementGetter(elementType);

        return (obj, mapper) -> {
            final ArrayData row = (ArrayData) obj;
            final ArrayNode arrayNode = mapper.createArrayNode();

            IntStream.range(0, row.size())
                    .mapToObj(
                            idx ->
                                    convertArrayEntry(
                                            elementConverter, elementGetter, row, idx, mapper))
                    .forEach(arrayNode::add);

            return arrayNode;
        };
    }

    private Converter createMapConverter(LogicalType keyType, LogicalType valueType) {
        final Converter keyConverter = createConverter(keyType);
        final Converter valueConverter = createConverter(valueType);
        final ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        final ElementGetter valueGetter = ArrayData.createElementGetter(valueType);

        return (obj, mapper) -> {
            final MapData row = (MapData) obj;
            final ArrayData keyArray = row.keyArray();
            final ArrayData valueArray = row.valueArray();

            final ArrayNode arrayNode = mapper.createArrayNode();

            IntStream.range(0, row.size())
                    .mapToObj(
                            idx -> {
                                final JsonNode value =
                                        convertArrayEntry(
                                                valueConverter,
                                                valueGetter,
                                                valueArray,
                                                idx,
                                                mapper);
                                final JsonNode key =
                                        convertArrayEntry(
                                                keyConverter, keyGetter, keyArray, idx, mapper);
                                final ArrayNode mapEntry = mapper.createArrayNode();
                                mapEntry.add(key);
                                mapEntry.add(value);
                                return mapEntry;
                            })
                    .forEach(arrayNode::add);

            return arrayNode;
        };
    }

    private static JsonNode convertArrayEntry(
            Converter valueConverter,
            ElementGetter valueGetter,
            ArrayData valueArray,
            int idx,
            ObjectMapper mapper) {
        if (valueArray.isNullAt(idx)) {
            return mapper.nullNode();
        } else {
            Object field = valueGetter.getElementOrNull(valueArray, idx);
            return valueConverter.apply(field, mapper);
        }
    }

    private Converter convertRowConverter(List<LogicalType> fieldTypes) {
        final Converter[] fieldConverters =
                fieldTypes.stream().map(this::createConverter).toArray(Converter[]::new);
        final int fieldCount = fieldTypes.size();

        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes.get(i), i);
        }

        return (obj, mapper) -> {
            final RowData row = (RowData) obj;
            final ArrayNode arrayNode = mapper.createArrayNode();
            IntStream.range(0, fieldCount)
                    .mapToObj(
                            idx -> {
                                if (row.isNullAt(idx)) {
                                    return mapper.nullNode();
                                } else {
                                    Object field = fieldGetters[idx].getFieldOrNull(row);
                                    return fieldConverters[idx].apply(field, mapper);
                                }
                            })
                    .forEach(arrayNode::add);
            return arrayNode;
        };
    }

    private Converter convertAtomicFieldConverter(LogicalType fieldType) {
        @SuppressWarnings("unchecked")
        final CastExecutor<Object, StringData> castExecutor =
                (CastExecutor<Object, StringData>)
                        CastRuleProvider.create(
                                this.castRuleContext, fieldType, VarCharType.STRING_TYPE);
        if (castExecutor == null) {
            throw new IllegalStateException(
                    "Cannot create a cast executor for converting "
                            + fieldType
                            + " to string. This is a bug, please open an issue.");
        }

        return (obj, mapper) -> mapper.getNodeFactory().textNode(castExecutor.cast(obj).toString());
    }
}
