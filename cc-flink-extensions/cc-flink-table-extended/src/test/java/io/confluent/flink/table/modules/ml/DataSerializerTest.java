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

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.flink.table.modules.ml.formats.DataSerializer;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

/** Tests for DataSerializer. */
public class DataSerializerTest {
    @Test
    public void testGetSerializer() {
        LogicalType inputType = new VarCharType(5);
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(inputType);
        assertNotNull(serializer);
    }

    @Test
    public void testGetDeserializer() {
        LogicalType outputType = new VarCharType(5);
        DataSerializer.OutputDeserializer deserializer = DataSerializer.getDeserializer(outputType);
        assertNotNull(deserializer);
    }

    @Test
    public void testGetSerializerWithUnsupportedType() {
        LogicalType inputType = new TimestampType(3);
        // Check that the input type is not supported and throws an exception.
        assertThatThrownBy(() -> DataSerializer.getSerializer(inputType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported type for model input: TIMESTAMP(3)");
    }

    @Test
    public void testGetDeserializerWithUnsupportedType() {
        LogicalType outputType = new TimestampType(3);
        // Check that the output type is not supported and throws an exception.
        assertThatThrownBy(() -> DataSerializer.getDeserializer(outputType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported type for model output: TIMESTAMP(3)");
    }

    @Test
    public void testSerializeNull() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new NullType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, null);
        assertThat(serialized.isNull()).isTrue();
    }

    @Test
    public void testSerializeTinyInt() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new TinyIntType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, (byte) 5);
        assertThat(serialized.asInt()).isEqualTo(5);
    }

    @Test
    public void testSerializeSmallInt() {
        DataSerializer.InputSerializer serializer =
                DataSerializer.getSerializer(new SmallIntType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, (short) 5);
        assertThat(serialized.asInt()).isEqualTo(5);
    }

    @Test
    public void testSerializeBoolean() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new BooleanType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, true);
        assertThat(serialized.asBoolean()).isTrue();
    }

    @Test
    public void testSerializeInt() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new IntType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, 5);
        assertThat(serialized.asInt()).isEqualTo(5);
    }

    @Test
    public void testSerializeBigInt() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new BigIntType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, 5L);
        assertThat(serialized.asInt()).isEqualTo(5);
    }

    @Test
    public void testSerializeFloat() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new FloatType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, 5.5f);
        assertThat(serialized.asDouble()).isEqualTo(5.5);
    }

    @Test
    public void testSerializeDouble() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new DoubleType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, 5.5);
        assertThat(serialized.asDouble()).isEqualTo(5.5);
    }

    @Test
    public void testSerializeChar() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new CharType(5));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, "hello");
        assertThat(serialized.asText()).isEqualTo("hello");
    }

    @Test
    public void testSerializeVarChar() {
        DataSerializer.InputSerializer serializer =
                DataSerializer.getSerializer(new VarCharType(5));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, "hello");
        assertThat(serialized.asText()).isEqualTo("hello");
    }

    @Test
    public void testSerializeBinary() {
        DataSerializer.InputSerializer serializer = DataSerializer.getSerializer(new BinaryType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, new byte[] {1, 2, 3});
        assertThat(serialized.asText()).isEqualTo("AQID");
    }

    @Test
    public void testSerializeVarBinary() {
        DataSerializer.InputSerializer serializer =
                DataSerializer.getSerializer(new VarBinaryType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, new byte[] {1, 2, 3});
        assertThat(serialized.asText()).isEqualTo("AQID");
    }

    @Test
    public void testSerializeDecimal() {
        DataSerializer.InputSerializer serializer =
                DataSerializer.getSerializer(new DecimalType(5, 2));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, new BigDecimal("7.1"));
        assertThat(serialized.asText()).isEqualTo("7.1");
    }

    @Test
    public void testSerializeArray() {
        DataSerializer.InputSerializer serializer =
                DataSerializer.getSerializer(new ArrayType(new IntType()));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, new Integer[] {1, 2, 3});
        assertThat(serialized.isArray()).isTrue();
        assertThat(serialized.size()).isEqualTo(3);
        assertThat(serialized.get(0).asInt()).isEqualTo(1);
        assertThat(serialized.get(1).asInt()).isEqualTo(2);
        assertThat(serialized.get(2).asInt()).isEqualTo(3);
    }

    @Test
    public void testSerializeRow() {
        ArrayList<RowType.RowField> rowFields = new ArrayList<>();
        rowFields.add(new RowType.RowField("f0", new IntType()));
        rowFields.add(new RowType.RowField("f1", new VarCharType(5)));
        DataSerializer.InputSerializer serializer =
                DataSerializer.getSerializer(new RowType(rowFields));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = serializer.convert(mapper, null, Row.of(5, "hello"));
        assertThat(serialized.isObject()).isTrue();
        assertThat(serialized.get("f0").asInt()).isEqualTo(5);
        assertThat(serialized.get("f1").asText()).isEqualTo("hello");
    }

    @Test
    public void testDeserializeNull() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new NullType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.nullNode();
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isNull();
    }

    @Test
    public void testDeserializeTinyInt() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new TinyIntType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("5");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo((byte) 5);
    }

    @Test
    public void testDeserializeSmallInt() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new SmallIntType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("5");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo((short) 5);
    }

    @Test
    public void testDeserializeBoolean() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new BooleanType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("true");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(true);
    }

    @Test
    public void testDeserializeInt() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new IntType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("5");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(5);
    }

    @Test
    public void testDeserializeBigInt() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new BigIntType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("5");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(5L);
    }

    @Test
    public void testDeserializeFloat() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new FloatType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("5.5");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(5.5f);
    }

    @Test
    public void testDeserializeDouble() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new DoubleType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("5.5");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(5.5);
    }

    @Test
    public void testDeserializeChar() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new CharType(5));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("\"hello\"");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized.toString()).isEqualTo("hello");
    }

    @Test
    public void testDeserializeVarChar() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new VarCharType(5));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("\"hello\"");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized.toString()).isEqualTo("hello");
    }

    @Test
    public void testDeserializeBinary() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new BinaryType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("\"AQID\"");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    public void testDeserializeVarBinary() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new VarBinaryType());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("\"AQID\"");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    public void testDeserializeDecimal() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new DecimalType(5, 2));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("7.1");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(new BigDecimal("7.1"));
    }

    @Test
    public void testDeserializeArray() throws IOException {
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new ArrayType(new IntType()));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("[1, 2, 3]");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized.equals(new int[] {1, 2, 3}));

        // Test nested array
        serialized = mapper.readTree("[[1, 2, 3]]");
        deserialized = deserializer.convert(serialized);
        assertThat(deserialized.equals(new int[] {1, 2, 3}));

        // Array nested too deeply should throw an exception
        final JsonNode deepArray = mapper.readTree("[[[1, 2, 3]]]");
        assertThatThrownBy(() -> deserializer.convert(deepArray))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "ML Predict attempted to deserialize an nested array of depth 1 from a json array of depth 3");

        // Non-array input should throw an exception
        final JsonNode nonArray = mapper.readTree("1");
        assertThatThrownBy(() -> deserializer.convert(nonArray))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "ML Predict attempted to deserialize an array from a non-array json object");
    }

    @Test
    public void testDeserializeRow() throws IOException {
        ArrayList<RowType.RowField> rowFields = new ArrayList<>();
        rowFields.add(new RowType.RowField("f0", new IntType()));
        rowFields.add(new RowType.RowField("f1", new VarCharType(5)));
        DataSerializer.OutputDeserializer deserializer =
                DataSerializer.getDeserializer(new RowType(rowFields));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode serialized = mapper.readTree("{\"f0\": 5, \"f1\": \"hello\"}");
        Object deserialized = deserializer.convert(serialized);
        assertThat(deserialized).isEqualTo(Row.of(5, "hello"));
    }
}
