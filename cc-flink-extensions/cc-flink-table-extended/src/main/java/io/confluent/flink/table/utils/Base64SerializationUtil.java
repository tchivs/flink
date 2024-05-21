/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

/** Util to de/serialize with Java serialization and Base64. */
public class Base64SerializationUtil {

    private Base64SerializationUtil() {}

    public static String serialize(ThrowingConsumer<ObjectOutputStream, Exception> oosWriter)
            throws Exception {
        try (ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oosWriter.accept(oos);
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        }
    }

    public static <T> T deserialize(
            String base64String, FunctionWithException<ObjectInputStream, T, Exception> oisReader)
            throws Exception {
        byte[] decodedBytes = Base64.getDecoder().decode(base64String);
        try (ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStreamWithPos(decodedBytes))) {
            return oisReader.apply(ois);
        }
    }
}
