/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.api;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Defines serialization apis used by UDFs. */
public interface UdfSerialization {

    /**
     * Serializes the given call arguments into a {@link ByteString} to send them to the remote UDF
     * service.
     *
     * @param args the arguments.
     * @return the serialized arguments.
     * @throws IOException on serialization error.
     */
    ByteString serializeArguments(Object[] args) throws IOException;

    /**
     * Deserializes the serialized call arguments from the given {@link ByteBuffer}. This can be
     * used by the remote service to deserialize received arguments.
     *
     * @param serialized the serialized call arguments.
     * @param output output array for the deserialized arguments. Length must be equal to
     *     argumentSerializers.size(),
     * @throws IOException on deserialization error.
     */
    void deserializeArguments(ByteBuffer serialized, Object[] output) throws IOException;

    /**
     * Serializes the given return value into a {@link ByteString} to send it back to the TM.
     *
     * @param returnValue the return value.
     * @return the serialized return value.
     * @throws IOException on serialization error.
     */
    ByteString serializeReturnValue(Object returnValue) throws IOException;

    /**
     * Deserializes the serialized return value from the given {@link ByteBuffer}. This can be used
     * by the TM to deserialize received return value from the remote UDF service.
     *
     * @param serializedReturnValue the serialized return value.
     * @return the deserialized return value.
     * @throws IOException on deserialization error.
     */
    Object deserializeReturnValue(ByteString serializedReturnValue) throws IOException;

    /**
     * Serializes the given {@link RemoteUdfSpec} into a {@link ByteString} to send the specs from
     * TM to the remote UDF service when creating a function instance.
     *
     * @param spec the spec to serialize.
     * @return the serialized spec.
     * @throws IOException on serialization error.
     */
    ByteString serializeRemoteUdfSpec(RemoteUdfSpec spec) throws IOException;

    /** Called when the object is done being used. */
    void close();
}
