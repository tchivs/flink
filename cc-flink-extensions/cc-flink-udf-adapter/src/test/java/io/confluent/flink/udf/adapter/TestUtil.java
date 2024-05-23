/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import com.google.protobuf.UnsafeByteOperations;
import io.confluent.flink.udf.adapter.api.FlinkConfiguration;
import io.confluent.flink.udf.adapter.api.RemoteUdfSerialization;
import io.confluent.flink.udf.adapter.api.RemoteUdfSpec;
import io.confluent.function.runtime.core.Context;
import io.confluent.function.runtime.core.RequestInvocationException;

import java.util.List;
import java.util.stream.Collectors;

/** Test utilities for the adapter. */
public class TestUtil {

    public static final Context DUMMY_CONTEXT = new SimpleContext();

    public static void writeSerializedOpenPayload(
            String organization,
            String environment,
            String pluginId,
            String pluginVersionId,
            LogicalType retType,
            List<LogicalType> argTypes,
            String functionClass,
            boolean isDeterministic,
            DataOutputSerializer out,
            Configuration configuration)
            throws Exception {
        new RemoteUdfSpec(
                        organization,
                        environment,
                        pluginId,
                        pluginVersionId,
                        functionClass,
                        isDeterministic,
                        DataTypeUtils.toInternalDataType(retType),
                        argTypes.stream()
                                .map(DataTypeUtils::toInternalDataType)
                                .collect(Collectors.toList()))
                .serialize(out);
        FlinkConfiguration.serialize(configuration, out);
    }

    public static byte[] createSerializedOpenPayload(
            String organization,
            String environment,
            String pluginId,
            String pluginVersionId,
            LogicalType retType,
            List<LogicalType> argTypes,
            String functionClass,
            boolean isDeterministic,
            Configuration configuration)
            throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(512);
        writeSerializedOpenPayload(
                organization,
                environment,
                pluginId,
                pluginVersionId,
                retType,
                argTypes,
                functionClass,
                isDeterministic,
                out,
                configuration);
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

    public static RemoteUdfSerialization createSerializers(ScalarFunctionHandler function)
            throws RequestInvocationException {
        return new RemoteUdfSerialization(
                function.getCallAdapter().getReturnValueSerializer().duplicate(),
                function.getCallAdapter().getArgumentSerializers().stream()
                        .map(TypeSerializer::duplicate)
                        .collect(Collectors.toList()));
    }

    static class SimpleContext implements Context {
        // TODO update with  useful information available within the Function Platform execution
        // environment.
    }
}
