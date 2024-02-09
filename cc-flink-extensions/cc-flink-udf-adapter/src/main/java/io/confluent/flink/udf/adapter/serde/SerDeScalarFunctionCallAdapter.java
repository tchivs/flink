/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.serde;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import io.confluent.flink.table.modules.remoteudf.RemoteUdfSpec;
import io.confluent.flink.udf.adapter.ScalarFunctionInstanceCallAdapter;
import io.confluent.flink.udf.adapter.codegen.ScalarFunctionAdapterGenerator;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapter layer that decorates a {@link ScalarFunctionInstanceCallAdapter} with de/serialization
 * support for remote transfer of arguments and return value.
 */
public class SerDeScalarFunctionCallAdapter {

    /** Decorated adapter to which calls are forwarded. */
    private final ScalarFunctionInstanceCallAdapter callAdapter;

    /** Type serializers for the function call return value. */
    private final TypeSerializer<Object> returnValueSerializer;

    /** Type serializers for the function call arguments. */
    private final List<TypeSerializer<Object>> argumentSerializers;

    /** Reused {@link DataInputDeserializer} instance. */
    private final DataInputDeserializer inputDeserializer;

    /** Reused {@link DataOutputSerializer} instance. */
    private final DataOutputSerializer outputSerializer;

    /** Reused buffer to pass argument objects into the function call. */
    private final Object[] argsArray;

    public static SerDeScalarFunctionCallAdapter create(
            String instanceId, byte[] openPayload, ClassLoader classLoader) throws Exception {

        DataInputDeserializer in = new DataInputDeserializer(openPayload);
        RemoteUdfSpec remoteUdfSpec = RemoteUdfSpec.deserialize(in, classLoader);

        LogicalType returnLogicalType = remoteUdfSpec.getReturnType().getLogicalType();
        List<LogicalType> argumentLogicalTypes =
                remoteUdfSpec.getArgumentTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());
        ScalarFunction functionInstance =
                (ScalarFunction)
                        Class.forName(remoteUdfSpec.getFunctionClassName())
                                .getDeclaredConstructor()
                                .newInstance();

        Class<?> returnClass = LogicalTypeUtils.toInternalConversionClass(returnLogicalType);
        List<Class<?>> argumentInternalConversionClasses =
                argumentLogicalTypes.stream()
                        .map(LogicalTypeUtils::toInternalConversionClass)
                        .collect(Collectors.toList());
        ScalarFunctionInstanceCallAdapter instanceCallAdapter =
                ScalarFunctionAdapterGenerator.generate(
                        instanceId,
                        functionInstance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        returnClass,
                        argumentInternalConversionClasses,
                        classLoader);

        TypeSerializer<Object> returnSerializer = InternalSerializers.create(returnLogicalType);
        List<TypeSerializer<Object>> argumentSerializers =
                argumentLogicalTypes.stream()
                        .map(InternalSerializers::create)
                        .collect(Collectors.toList());
        return create(instanceCallAdapter, returnSerializer, argumentSerializers);
    }

    public static SerDeScalarFunctionCallAdapter create(
            ScalarFunctionInstanceCallAdapter callAdapter,
            TypeSerializer<Object> returnSerializer,
            List<TypeSerializer<Object>> argumentSerializers) {
        return new SerDeScalarFunctionCallAdapter(
                callAdapter,
                new DataInputDeserializer(),
                new DataOutputSerializer(argumentSerializers.size() * 16),
                returnSerializer,
                argumentSerializers);
    }

    private SerDeScalarFunctionCallAdapter(
            ScalarFunctionInstanceCallAdapter callAdapter,
            DataInputDeserializer inputDeserializer,
            DataOutputSerializer outputSerializer,
            TypeSerializer<Object> returnValueSerializer,
            List<TypeSerializer<Object>> argumentSerializers) {
        this.callAdapter = callAdapter;
        this.inputDeserializer = inputDeserializer;
        this.outputSerializer = outputSerializer;
        this.returnValueSerializer = returnValueSerializer;
        this.argumentSerializers = argumentSerializers;
        this.argsArray = new Object[argumentSerializers.size()];
    }

    public byte[] call(byte[] serializedArgs) throws Throwable {
        inputDeserializer.setBuffer(serializedArgs);
        for (int i = 0; i < argumentSerializers.size(); ++i) {
            argsArray[i] = argumentSerializers.get(i).deserialize(inputDeserializer);
        }

        Object returnValue = callAdapter.call(argsArray);

        outputSerializer.clear();
        returnValueSerializer.serialize(returnValue, outputSerializer);
        // TODO can we avoid the copy?
        return outputSerializer.getCopyOfBuffer();
    }

    public void open() throws Exception {
        callAdapter.open();
    }

    public void close() throws Exception {
        callAdapter.close();
    }

    public TypeSerializer<Object> getReturnValueSerializer() {
        return returnValueSerializer;
    }

    public List<TypeSerializer<Object>> getArgumentSerializers() {
        return argumentSerializers;
    }
}
