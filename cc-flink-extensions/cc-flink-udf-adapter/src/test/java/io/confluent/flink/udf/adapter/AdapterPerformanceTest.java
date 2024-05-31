/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;

import io.confluent.flink.udf.adapter.examples.SumScalarFunction;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Small performance test to compare the overhead of the adapter and serialization against a direct
 * call.
 */
public class AdapterPerformanceTest {

    private static final int ITERATIONS = 20;
    private static final int CALLS = 10_000_000;

    @Test
    public void testAdapter() throws Throwable {
        for (int k = 0; k < ITERATIONS; ++k) {
            final String pluginUUID = UUID.randomUUID().toString();
            final String pluginVersionUUID = UUID.randomUUID().toString();
            final LogicalType retType = new IntType();
            final List<LogicalType> argTypes = Arrays.asList(new IntType(), new IntType());
            final String functionClass = SumScalarFunction.class.getName();
            final TypeSerializer<Object> retTypeSerializer =
                    NullableSerializer.wrapIfNullIsNotSupported(
                            InternalSerializers.create(retType), false);
            final List<TypeSerializer<Object>> argTypeSerializers =
                    argTypes.stream()
                            .map(InternalSerializers::create)
                            .map(ts -> NullableSerializer.wrapIfNullIsNotSupported(ts, false))
                            .collect(Collectors.toList());

            DataOutputSerializer out = new DataOutputSerializer(8);
            TestUtil.writeSerializedOpenPayload(
                    "testOrg",
                    "testEnv",
                    pluginUUID,
                    pluginVersionUUID,
                    retType,
                    argTypes,
                    functionClass,
                    true,
                    out,
                    new Configuration());
            byte[] openPayloadBytes = out.getCopyOfBuffer();
            final ScalarFunctionHandler handler = new ScalarFunctionHandler();
            handler.open(openPayloadBytes, TestUtil.DUMMY_CONTEXT);
            int sum = 0;
            DataInputDeserializer in = new DataInputDeserializer();
            long t = System.currentTimeMillis();
            for (int i = 0; i < CALLS; ++i) {
                out.clear();
                for (TypeSerializer<Object> argTypeSerializer : argTypeSerializers) {
                    argTypeSerializer.serialize(i, out);
                }
                byte[] retBytes =
                        handler.handleRequest(out.getSharedBuffer(), TestUtil.DUMMY_CONTEXT);
                in.setBuffer(retBytes);
                Object resultObject = retTypeSerializer.deserialize(in);
                sum += (Integer) resultObject;
            }
            handler.close(new byte[0], TestUtil.DUMMY_CONTEXT);
            System.out.println(System.currentTimeMillis() - t);
            System.out.println(sum);
        }
    }

    @Test
    public void testDirect() {
        for (int k = 0; k < ITERATIONS; ++k) {
            final SumScalarFunction sumScalarFunction = new SumScalarFunction();
            long t = System.currentTimeMillis();
            int sum = 0;
            for (int i = 0; i < CALLS; ++i) {
                sum += sumScalarFunction.eval(i, i);
            }
            System.out.println(System.currentTimeMillis() - t);
            System.out.println(sum);
        }
    }
}
