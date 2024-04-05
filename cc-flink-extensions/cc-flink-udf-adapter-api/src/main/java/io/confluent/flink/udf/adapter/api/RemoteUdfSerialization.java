/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.api;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** Serialization support for communication between the TM and the remote UDF service. */
public class RemoteUdfSerialization {
    public RemoteUdfSerialization(
            TypeSerializer<Object> returnTypeSerializer,
            List<TypeSerializer<Object>> argumentSerializers) {
        this.returnTypeSerializer = returnTypeSerializer;
        this.argumentSerializers = argumentSerializers;
    }

    /** Cached instance for serializing. */
    private final DataOutputSerializer dataOutput = new DataOutputSerializer(128);
    /** Cached instance for deserializing. */
    private final DataInputDeserializer dataInput = new DataInputDeserializer();
    /** The serializer for the return value. */
    private final TypeSerializer<Object> returnTypeSerializer;
    /** The serializers for the argument values. */
    private final List<TypeSerializer<Object>> argumentSerializers;

    /**
     * Serializes the given call arguments into a {@link ByteString} to send them to the remote UDF
     * service.
     *
     * @param args the arguments.
     * @return the serialized arguments.
     * @throws IOException on serialization error.
     */
    public ByteString serializeArguments(Object[] args) throws IOException {
        Preconditions.checkArgument(argumentSerializers.size() == args.length);
        dataOutput.clear();
        for (int i = 0; i < args.length; ++i) {
            argumentSerializers.get(i).serialize(args[i], dataOutput);
        }
        return outputToByteString();
    }

    /**
     * Deserializes the serialized call arguments from the given {@link ByteBuffer}. This can be
     * used by the remote service to deserialize received arguments.
     *
     * @param serialized the serialized call arguments.
     * @param output output array for the deserialized arguments. Length must be equal to
     *     argumentSerializers.size(),
     * @throws IOException on deserialization error.
     */
    public void deserializeArguments(ByteBuffer serialized, Object[] output) throws IOException {
        Preconditions.checkArgument(argumentSerializers.size() == output.length);
        dataInput.setBuffer(serialized);
        for (int i = 0; i < argumentSerializers.size(); ++i) {
            output[i] = argumentSerializers.get(i).deserialize(dataInput);
        }
    }

    /**
     * Serializes the given return value into a {@link ByteString} to send it back to the TM.
     *
     * @param returnValue the return value.
     * @return the serialized return value.
     * @throws IOException on serialization error.
     */
    public ByteString serializeReturnValue(Object returnValue) throws IOException {
        dataOutput.clear();
        returnTypeSerializer.serialize(returnValue, dataOutput);
        return outputToByteString();
    }

    /**
     * Deserializes the serialized return value from the given {@link ByteBuffer}. This can be used
     * by the TM to deserialize received return value from the remote UDF service.
     *
     * @param serializedReturnValue the serialized return value.
     * @return the deserialized return value.
     * @throws IOException on deserialization error.
     */
    public Object deserializeReturnValue(ByteString serializedReturnValue) throws IOException {
        // TODO: check if it's worth avoiding this buffer copy for performance
        dataInput.setBuffer(serializedReturnValue.toByteArray());
        return returnTypeSerializer.deserialize(dataInput);
    }

    /**
     * Serializes the given {@link RemoteUdfSpec} into a {@link ByteString} to send the specs from
     * TM to the remote UDF service when creating a function instance.
     *
     * @param spec the spec to serialize.
     * @return the serialized spec.
     * @throws IOException on serialization error.
     */
    public ByteString serializeRemoteUdfSpec(RemoteUdfSpec spec) throws IOException {
        dataOutput.clear();
        spec.serialize(dataOutput);
        return outputToByteString();
    }

    private ByteString outputToByteString() {
        return UnsafeByteOperations.unsafeWrap(
                dataOutput.getSharedBuffer(), 0, dataOutput.length());
    }
}
