/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowDataJsonFormatter}. */
@Confluent
public class RowDataJsonFormatterTest {

    private static Stream<Arguments> params() {
        DataType rowType1 =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.FLOAT().nullable()),
                        DataTypes.FIELD("f1", DataTypes.TIMESTAMP().nullable()),
                        DataTypes.FIELD(
                                "r2",
                                DataTypes.ROW(DataTypes.FIELD("rf0", DataTypes.BIGINT().nullable()))
                                        .nullable()));

        DataType rowTypeWithCollectionTypes =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "f0", DataTypes.ARRAY(DataTypes.BIGINT().nullable()).nullable()),
                        DataTypes.FIELD(
                                "f1",
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT().nullable()))
                                        .nullable()),
                        DataTypes.FIELD(
                                "f2",
                                DataTypes.MAP(
                                                DataTypes.STRING().nullable(),
                                                DataTypes.BIGINT().nullable())
                                        .nullable()),
                        DataTypes.FIELD(
                                "f3",
                                DataTypes.MULTISET(DataTypes.BOOLEAN().nullable()).nullable()));

        Map<String, Long> mapWithNulls = new HashMap<>();
        mapWithNulls.put(null, 1L);
        mapWithNulls.put("ABC", null);

        Map<Boolean, Integer> multisetWithNulls = new HashMap<>();
        multisetWithNulls.put(null, 1);
        multisetWithNulls.put(true, 3);
        multisetWithNulls.put(false, 5);

        Map<String, Long> mapStringToLong = new HashMap<>();
        mapStringToLong.put("ABC", 1L);
        mapStringToLong.put("DEF", 5L);

        Map<Boolean, Integer> mapBooleanToInt = new HashMap<>();
        mapBooleanToInt.put(true, 1);
        mapBooleanToInt.put(false, 4);

        return Stream.of(
                Arguments.of(
                        rowType1,
                        dataForRowType1(
                                1234.56f,
                                TimestampData.fromEpochMillis(12345),
                                dataForRowType1NestedRow(12345L)),
                        "{\"row\":[\"1234.56\",\"1970-01-01 00:00:12.345000\",[\"12345\"]]}"),
                Arguments.of(
                        rowType1, dataForRowType1(null, null, null), "{\"row\":[null,null,null]}"),
                Arguments.of(
                        rowType1,
                        dataForRowType1(null, null, dataForRowType1NestedRow(null)),
                        "{\"row\":[null,null,[null]]}"),
                Arguments.of(
                        rowTypeWithCollectionTypes,
                        dataForRowTypeWithCollectionTypes(
                                new Long[] {1L, 5L},
                                Collections.singletonList(new Long[] {2L, 4L}),
                                mapStringToLong,
                                mapBooleanToInt),
                        "{\"row\":[[\"1\",\"5\"],[[\"2\",\"4\"]],[[\"ABC\",\"1\"],[\"DEF\",\"5\"]],[[\"FALSE\",\"4\"],[\"TRUE\",\"1\"]]]}"),
                Arguments.of(
                        rowTypeWithCollectionTypes,
                        dataForRowTypeWithCollectionTypes(null, null, null, null),
                        "{\"row\":[null,null,null,null]}"),
                Arguments.of(
                        rowTypeWithCollectionTypes,
                        dataForRowTypeWithCollectionTypes(
                                new Long[] {1L, null},
                                Arrays.asList(new Long[] {null, 4L}, null),
                                mapWithNulls,
                                multisetWithNulls),
                        "{\"row\":[[\"1\",null],[[null,\"4\"],null],[[null,\"1\"],[\"ABC\",null]],[[null,\"1\"],[\"FALSE\",\"5\"],[\"TRUE\",\"3\"]]]}"));
    }

    private static GenericRowData dataForRowTypeWithCollectionTypes(
            Long[] f0Value,
            List<Long[]> f1Value,
            Map<String, Long> f2Value,
            Map<Boolean, Integer> f3Value) {
        GenericRowData rowData = new GenericRowData(4);

        if (f0Value != null) {
            rowData.setField(0, new GenericArrayData(f0Value));
        }
        if (f1Value != null) {
            rowData.setField(
                    1,
                    new GenericArrayData(
                            f1Value.stream()
                                    .map(v -> v != null ? new GenericArrayData(v) : null)
                                    .toArray(GenericArrayData[]::new)));
        }

        if (f2Value != null) {
            Map<StringData, Long> map = new LinkedHashMap<>();
            f2Value.entrySet().stream()
                    .sorted(Entry.comparingByKey(Comparator.nullsFirst(Comparator.naturalOrder())))
                    .forEachOrdered(e -> map.put(StringData.fromString(e.getKey()), e.getValue()));
            rowData.setField(2, new GenericMapData(map));
        }

        if (f3Value != null) {
            Map<Boolean, Integer> map = new LinkedHashMap<>();
            f3Value.entrySet().stream()
                    .sorted(Entry.comparingByKey(Comparator.nullsFirst(Comparator.naturalOrder())))
                    .forEachOrdered(e -> map.put(e.getKey(), e.getValue()));
            rowData.setField(3, new GenericMapData(map));
        }
        return rowData;
    }

    private static GenericRowData dataForRowType1(
            Float f0Value, TimestampData f1Value, RowData r2Value) {
        GenericRowData rowData = new GenericRowData(3);

        rowData.setField(0, f0Value);
        rowData.setField(1, f1Value);
        rowData.setField(2, r2Value);
        return rowData;
    }

    private static GenericRowData dataForRowType1NestedRow(Long rf0Value) {
        GenericRowData nestedData = new GenericRowData(1);

        nestedData.setField(0, rf0Value);
        return nestedData;
    }

    @ParameterizedTest
    @MethodSource("params")
    void test(DataType type, RowData data, String expected) {
        RowDataJsonFormatter formatter =
                new RowDataJsonFormatter(
                        type.getLogicalType(),
                        ZoneId.of("UTC"),
                        this.getClass().getClassLoader(),
                        true);

        assertThat(formatter.convert(data)).isEqualTo(expected);
    }

    @ParameterizedTest
    @EnumSource(RowKind.class)
    void testRowKind(RowKind rowKind) {
        RowDataJsonFormatter formatter =
                new RowDataJsonFormatter(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT())).getLogicalType(),
                        ZoneId.of("UTC"),
                        this.getClass().getClassLoader(),
                        false);

        final GenericRowData rowData = new GenericRowData(rowKind, 1);
        rowData.setField(0, 1L);
        assertThat(formatter.convert(rowData))
                .isEqualTo(String.format("{\"op\":%d,\"row\":[\"1\"]}", rowKind.toByteValue()));
    }
}
