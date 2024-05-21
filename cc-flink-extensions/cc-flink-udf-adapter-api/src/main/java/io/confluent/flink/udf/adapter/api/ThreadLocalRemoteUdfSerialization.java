/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.api;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Thread safe version of {@link UdfSerialization} using thread locals. Note that there may be some
 * performance penalties from extra copies of data.
 */
public class ThreadLocalRemoteUdfSerialization implements UdfSerialization {
    private static final ThreadLocal<RemoteUdfSerialization> SERIALIZATION = new ThreadLocal<>();
    // Note this is not threadsafe, so will have to be cloned for each thread local
    private final TypeSerializer<Object> returnTypeSerializer;
    // Note this is not threadsafe, so will have to be cloned for each thread local
    private final List<TypeSerializer<Object>> argumentSerializers;

    public ThreadLocalRemoteUdfSerialization(
            TypeSerializer<Object> returnTypeSerializer,
            List<TypeSerializer<Object>> argumentSerializers) {
        this.returnTypeSerializer = returnTypeSerializer;
        this.argumentSerializers = argumentSerializers;
    }

    @Override
    public ByteString serializeArguments(Object[] args) throws IOException {
        RemoteUdfSerialization serialization = getUdfSerialization();
        return createCopy(serialization.serializeArguments(args));
    }

    @Override
    public void deserializeArguments(ByteBuffer serialized, Object[] output) throws IOException {
        RemoteUdfSerialization serialization = getUdfSerialization();
        serialization.deserializeArguments(serialized, output);
    }

    @Override
    public ByteString serializeReturnValue(Object returnValue) throws IOException {
        RemoteUdfSerialization serialization = getUdfSerialization();
        return createCopy(serialization.serializeReturnValue(returnValue));
    }

    @Override
    public Object deserializeReturnValue(ByteString serializedReturnValue) throws IOException {
        RemoteUdfSerialization serialization = getUdfSerialization();
        return serialization.deserializeReturnValue(serializedReturnValue);
    }

    @Override
    public ByteString serializeRemoteUdfSpec(RemoteUdfSpec spec) throws IOException {
        RemoteUdfSerialization serialization = getUdfSerialization();
        return createCopy(serialization.serializeRemoteUdfSpec(spec));
    }

    @Override
    public void close() {
        SERIALIZATION.remove();
    }

    /**
     * This is important for ensuring that we don't actually share buffers between invocations, even
     * if they are all done on the same thread. This could be an issue for retries which obviously
     * are not guaranteed to be issued in order.
     */
    private ByteString createCopy(ByteString byteString) {
        return ByteString.copyFrom(byteString.asReadOnlyByteBuffer());
    }

    private RemoteUdfSerialization getUdfSerialization() {
        // We first check to see if we have any thread local RemoteUdfSerialization. If not, then
        // we create a local copy.
        RemoteUdfSerialization serialization = SERIALIZATION.get();
        if (serialization == null) {
            // Note that the serializers are not threadsafe, so we synchronize on "this" while
            // duplicating each of them for use in the calling thread. Note that this
            // synchronization is only used during creation time and afterward, we will use our
            // thread local copy.
            synchronized (this) {
                serialization =
                        new RemoteUdfSerialization(
                                returnTypeSerializer.duplicate(),
                                argumentSerializers.stream()
                                        .map(TypeSerializer::duplicate)
                                        .collect(Collectors.toList()));
            }
            SERIALIZATION.set(serialization);
        }
        return serialization;
    }
}
