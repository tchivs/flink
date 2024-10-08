/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.legacy.api.TableColumn;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.types.LogicalTypeParserTest;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DescriptorProperties}. */
class DescriptorPropertiesTest {

    private static final String ARRAY_KEY = "my-array";
    private static final String FIXED_INDEXED_PROPERTY_KEY = "my-fixed-indexed-property";
    private static final String PROPERTY_1_KEY = "property-1";
    private static final String PROPERTY_2_KEY = "property-2";

    @Test
    void testEquals() {
        DescriptorProperties properties1 = new DescriptorProperties();
        properties1.putString("hello1", "12");
        properties1.putString("hello2", "13");
        properties1.putString("hello3", "14");

        DescriptorProperties properties2 = new DescriptorProperties();
        properties2.putString("hello1", "12");
        properties2.putString("hello2", "13");
        properties2.putString("hello3", "14");

        DescriptorProperties properties3 = new DescriptorProperties();
        properties3.putString("hello1", "12");
        properties3.putString("hello3", "14");
        properties3.putString("hello2", "13");

        assertThat(properties2).isEqualTo(properties1);

        assertThat(properties3).isEqualTo(properties1);
    }

    @Test
    void testMissingArray() {
        DescriptorProperties properties = new DescriptorProperties();

        testArrayValidation(properties, 0, Integer.MAX_VALUE);
    }

    @Test
    void testArrayValues() {
        DescriptorProperties properties = new DescriptorProperties();

        properties.putString(ARRAY_KEY + ".0", "12");
        properties.putString(ARRAY_KEY + ".1", "42");
        properties.putString(ARRAY_KEY + ".2", "66");

        testArrayValidation(properties, 1, Integer.MAX_VALUE);

        assertThat(properties.getArray(ARRAY_KEY, properties::getInt))
                .isEqualTo(Arrays.asList(12, 42, 66));
    }

    @Test
    void testArraySingleValue() {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putString(ARRAY_KEY, "12");

        testArrayValidation(properties, 1, Integer.MAX_VALUE);

        assertThat(properties.getArray(ARRAY_KEY, properties::getInt))
                .isEqualTo(Collections.singletonList(12));
    }

    @Test
    void testArrayInvalidValues() {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putString(ARRAY_KEY + ".0", "12");
        properties.putString(ARRAY_KEY + ".1", "66");
        properties.putString(ARRAY_KEY + ".2", "INVALID");

        assertThatThrownBy(() -> testArrayValidation(properties, 1, Integer.MAX_VALUE))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testArrayInvalidSingleValue() {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putString(ARRAY_KEY, "INVALID");

        assertThatThrownBy(() -> testArrayValidation(properties, 1, Integer.MAX_VALUE))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testInvalidMissingArray() {
        DescriptorProperties properties = new DescriptorProperties();

        assertThatThrownBy(() -> testArrayValidation(properties, 1, Integer.MAX_VALUE))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testInvalidFixedIndexedProperties() {
        DescriptorProperties property = new DescriptorProperties();
        List<List<String>> list = new ArrayList<>();
        list.add(Arrays.asList("1", "string"));
        list.add(Arrays.asList("INVALID", "string"));
        property.putIndexedFixedProperties(
                FIXED_INDEXED_PROPERTY_KEY, Arrays.asList(PROPERTY_1_KEY, PROPERTY_2_KEY), list);
        assertThatThrownBy(() -> testFixedIndexedPropertiesValidation(property))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testRemoveKeys() {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putString("hello1", "12");
        properties.putString("hello2", "13");
        properties.putString("hello3", "14");

        DescriptorProperties actual = properties.withoutKeys(Arrays.asList("hello1", "hello3"));

        DescriptorProperties expected = new DescriptorProperties();
        expected.putString("hello2", "13");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testPrefixedMap() {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putString("hello1", "12");
        properties.putString("hello2", "13");
        properties.putString("hello3", "14");

        Map<String, String> actual = properties.asPrefixedMap("prefix.");

        DescriptorProperties expected = new DescriptorProperties();
        expected.putString("prefix.hello1", "12");
        expected.putString("prefix.hello2", "13");
        expected.putString("prefix.hello3", "14");

        assertThat(actual).isEqualTo(expected.asMap());
    }

    @Test
    void testTableSchema() {
        TableSchema schema =
                TableSchema.builder()
                        .add(TableColumn.physical("f0", DataTypes.BIGINT().notNull()))
                        .add(
                                TableColumn.physical(
                                        "f1",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("q1", DataTypes.STRING()),
                                                DataTypes.FIELD("q2", DataTypes.TIMESTAMP(9)))))
                        .add(TableColumn.physical("f2", DataTypes.STRING().notNull()))
                        .add(TableColumn.computed("f3", DataTypes.BIGINT().notNull(), "f0 + 1"))
                        .add(TableColumn.physical("f4", DataTypes.DECIMAL(10, 3)))
                        .add(TableColumn.metadata("f5", DataTypes.DECIMAL(10, 3)))
                        .add(TableColumn.metadata("f6", DataTypes.INT(), "other.key"))
                        .add(TableColumn.metadata("f7", DataTypes.INT(), true))
                        .add(TableColumn.metadata("f8", DataTypes.INT(), "other.key", true))
                        .watermark(
                                "f1.q2", "`f1`.`q2` - INTERVAL '5' SECOND", DataTypes.TIMESTAMP(3))
                        .primaryKey("constraint1", new String[] {"f0", "f2"})
                        .build();

        DescriptorProperties properties = new DescriptorProperties();
        properties.putTableSchema("schema", schema);
        Map<String, String> actual = properties.asMap();
        Map<String, String> expected = new HashMap<>();

        expected.put("schema.0.name", "f0");
        expected.put("schema.0.data-type", "BIGINT NOT NULL");

        expected.put("schema.1.name", "f1");
        expected.put("schema.1.data-type", "ROW<`q1` VARCHAR(2147483647), `q2` TIMESTAMP(9)>");

        expected.put("schema.2.name", "f2");
        expected.put("schema.2.data-type", "VARCHAR(2147483647) NOT NULL");

        expected.put("schema.3.name", "f3");
        expected.put("schema.3.data-type", "BIGINT NOT NULL");
        expected.put("schema.3.expr", "f0 + 1");

        expected.put("schema.4.name", "f4");
        expected.put("schema.4.data-type", "DECIMAL(10, 3)");

        expected.put("schema.5.name", "f5");
        expected.put("schema.5.data-type", "DECIMAL(10, 3)");
        expected.put("schema.5.metadata", "f5");
        expected.put("schema.5.virtual", "false");

        expected.put("schema.6.name", "f6");
        expected.put("schema.6.data-type", "INT");
        expected.put("schema.6.metadata", "other.key");
        expected.put("schema.6.virtual", "false");

        expected.put("schema.7.name", "f7");
        expected.put("schema.7.data-type", "INT");
        expected.put("schema.7.metadata", "f7");
        expected.put("schema.7.virtual", "true");

        expected.put("schema.8.name", "f8");
        expected.put("schema.8.data-type", "INT");
        expected.put("schema.8.metadata", "other.key");
        expected.put("schema.8.virtual", "true");

        expected.put("schema.watermark.0.rowtime", "f1.q2");
        expected.put("schema.watermark.0.strategy.expr", "`f1`.`q2` - INTERVAL '5' SECOND");
        expected.put("schema.watermark.0.strategy.data-type", "TIMESTAMP(3)");

        expected.put("schema.primary-key.name", "constraint1");
        expected.put("schema.primary-key.columns", "f0,f2");

        assertThat(actual).isEqualTo(expected);

        TableSchema restored = properties.getTableSchema("schema");
        assertThat(restored).isEqualTo(schema);
    }

    @Test
    void testLegacyTableSchema() {
        DescriptorProperties properties = new DescriptorProperties();
        Map<String, String> map = new HashMap<>();
        map.put("schema.0.name", "f0");
        map.put("schema.0.type", "VARCHAR");
        map.put("schema.1.name", "f1");
        map.put("schema.1.type", "BIGINT");
        map.put("schema.2.name", "f2");
        map.put("schema.2.type", "DECIMAL");
        map.put("schema.3.name", "f3");
        map.put("schema.3.type", "TIMESTAMP");
        map.put("schema.4.name", "f4");
        map.put("schema.4.type", "MAP<TINYINT, SMALLINT>");
        map.put("schema.5.name", "f5");
        map.put("schema.5.type", "ANY<java.lang.Class>");
        map.put("schema.6.name", "f6");
        map.put("schema.6.type", "PRIMITIVE_ARRAY<DOUBLE>");
        map.put("schema.7.name", "f7");
        map.put("schema.7.type", "OBJECT_ARRAY<TIME>");
        map.put("schema.8.name", "f8");
        map.put("schema.8.type", "ROW<q1 VARCHAR, q2 DATE>");
        map.put("schema.9.name", "f9");
        map.put("schema.9.type", "POJO<org.apache.flink.table.types.LogicalTypeParserTest$MyPojo>");
        properties.putProperties(map);
        TableSchema restored = properties.getTableSchema("schema");

        TableSchema expected =
                TableSchema.builder()
                        .field("f0", Types.STRING)
                        .field("f1", Types.LONG)
                        .field("f2", Types.BIG_DEC)
                        .field("f3", Types.SQL_TIMESTAMP)
                        .field("f4", Types.MAP(Types.BYTE, Types.SHORT))
                        .field("f5", Types.GENERIC(Class.class))
                        .field("f6", Types.PRIMITIVE_ARRAY(Types.DOUBLE))
                        .field("f7", Types.OBJECT_ARRAY(Types.SQL_TIME))
                        .field(
                                "f8",
                                Types.ROW_NAMED(
                                        new String[] {"q1", "q2"}, Types.STRING, Types.SQL_DATE))
                        .field("f9", Types.POJO(LogicalTypeParserTest.MyPojo.class))
                        .build();

        assertThat(restored).isEqualTo(expected);
    }

    private void testArrayValidation(
            DescriptorProperties properties, int minLength, int maxLength) {
        Consumer<String> validator = key -> properties.validateInt(key, false);

        properties.validateArray(ARRAY_KEY, validator, minLength, maxLength);
    }

    private void testFixedIndexedPropertiesValidation(DescriptorProperties properties) {

        Map<String, Consumer<String>> validatorMap = new HashMap<>();

        // PROPERTY_1 should be Int
        Consumer<String> validator1 = key -> properties.validateInt(key, false);
        validatorMap.put(PROPERTY_1_KEY, validator1);
        // PROPERTY_2 should be String
        Consumer<String> validator2 = key -> properties.validateString(key, false);
        validatorMap.put(PROPERTY_2_KEY, validator2);

        properties.validateFixedIndexedProperties(FIXED_INDEXED_PROPERTY_KEY, false, validatorMap);
    }
}
