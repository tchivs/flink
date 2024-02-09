/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import com.google.protobuf.UnsafeByteOperations;
import io.confluent.flink.table.modules.remoteudf.RemoteUdfSerialization;
import io.confluent.flink.table.modules.remoteudf.RemoteUdfSpec;
import io.confluent.function.runtime.core.Context;

import java.util.List;
import java.util.stream.Collectors;

/** Test utilities for the adapter. */
public class TestUtil {

    public static final Context DUMMY_CONTEXT = new SimpleContext();

    public static void writeSerializedOpenPayload(
            String functionId,
            LogicalType retType,
            List<LogicalType> argTypes,
            String functionClass,
            DataOutputSerializer out)
            throws Exception {
        new RemoteUdfSpec(
                        functionId,
                        functionClass,
                        DataTypeUtils.toInternalDataType(retType),
                        argTypes.stream()
                                .map(DataTypeUtils::toInternalDataType)
                                .collect(Collectors.toList()))
                .serialize(out);
    }

    public static byte[] createSerializedOpenPayload(
            String callerUUID,
            LogicalType retType,
            List<LogicalType> argTypes,
            String functionClass)
            throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(512);
        writeSerializedOpenPayload(callerUUID, retType, argTypes, functionClass, out);
        return out.getCopyOfBuffer();
    }

    public static Object testInvoke(ScalarFunctionHandler function, Object[] args)
            throws Throwable {

        RemoteUdfSerialization serialization = createSerializers(function);

        return testInvoke(function, args, serialization);
    }

    public static Object testInvoke(
            ScalarFunctionHandler function, Object[] args, RemoteUdfSerialization serialization)
            throws Throwable {
        return serialization.deserializeReturnValue(
                UnsafeByteOperations.unsafeWrap(
                        function.handleRequest(
                                serialization.serializeArguments(args).toByteArray(),
                                DUMMY_CONTEXT)));
    }

    public static RemoteUdfSerialization createSerializers(ScalarFunctionHandler function) {
        return new RemoteUdfSerialization(
                function.getCallAdapter().getReturnValueSerializer(),
                function.getCallAdapter().getArgumentSerializers());
    }

    static class SimpleContext implements Context {
        // TODO update with  useful information available within the Function Platform execution
        // environment.
    }
}
