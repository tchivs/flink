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

package org.apache.flink.core.memory;

import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/** Test suite for the {@link DataInputDeserializer} class. */
public class DataInputDeserializerTest {

    interface Factory extends Function<DataOutputSerializer, DataInputDeserializer> {}

    final DataOutputSerializer out = new DataOutputSerializer(128);

    @Test
    public void testAvailable() throws Exception {
        byte[] bytes;
        DataInputDeserializer dis;

        bytes = new byte[] {};
        dis = new DataInputDeserializer(bytes, 0, bytes.length);
        Assert.assertEquals(bytes.length, dis.available());

        bytes = new byte[] {1, 2, 3};
        dis = new DataInputDeserializer(bytes, 0, bytes.length);
        Assert.assertEquals(bytes.length, dis.available());

        dis.readByte();
        Assert.assertEquals(2, dis.available());
        dis.readByte();
        Assert.assertEquals(1, dis.available());
        dis.readByte();
        Assert.assertEquals(0, dis.available());

        try {
            dis.readByte();
            Assert.fail("Did not throw expected IOException");
        } catch (IOException e) {
            // ignore
        }
        Assert.assertEquals(0, dis.available());
    }

    @Test
    public void testReadWithLenZero() throws IOException {
        byte[] bytes = new byte[0];
        DataInputDeserializer dis = new DataInputDeserializer(bytes, 0, bytes.length);
        Assert.assertEquals(0, dis.available());

        byte[] bytesForRead = new byte[0];
        Assert.assertEquals(0, dis.read(bytesForRead, 0, 0)); // do not throw when read with len 0
    }

    @Test
    void testEmpty() throws Exception {
        DataInputDeserializer inputView = prepareBulkData(x -> {});
        assertEquals(0, inputView.available());
        assertEquals(0, inputView.getPosition());
        try {
            inputView.readByte();
            fail();
        } catch (Exception expected) {
        }
    }

    @Test
    void readByte() throws Exception {
        List<Byte> testData =
                Arrays.asList((byte) 0, (byte) 1, Byte.MAX_VALUE, Byte.MIN_VALUE, (byte) -1);
        testHelper(testData, (element, out) -> out.writeByte(element), DataInput::readByte);
    }

    @Test
    void readChar() throws Exception {
        List<Character> testData =
                Arrays.asList(
                        (char) 0, (char) 1, Character.MAX_VALUE, Character.MIN_VALUE, (char) -1);
        testHelper(testData, (element, out) -> out.writeChar((int) element), DataInput::readChar);
    }

    @Test
    void readDouble() throws Exception {
        List<Double> testData =
                Arrays.asList(
                        0d,
                        1d,
                        Double.MAX_VALUE,
                        Double.MIN_VALUE,
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        -1d);
        testHelper(testData, (element, out) -> out.writeDouble(element), DataInput::readDouble);
    }

    @Test
    void readFloat() throws Exception {
        List<Float> testData =
                Arrays.asList(
                        0f,
                        1f,
                        Float.MAX_VALUE,
                        Float.MIN_VALUE,
                        Float.NEGATIVE_INFINITY,
                        Float.POSITIVE_INFINITY,
                        -1f);
        testHelper(testData, (element, out) -> out.writeFloat(element), DataInput::readFloat);
    }

    @Test
    void readInt() throws Exception {
        List<Integer> testData = Arrays.asList(0, 1, Integer.MAX_VALUE, Integer.MIN_VALUE, -1);
        testHelper(testData, (element, out) -> out.writeInt(element), DataInput::readInt);
    }

    @Test
    void readLong() throws Exception {
        List<Long> testData = Arrays.asList(0L, 1L, Long.MAX_VALUE, Long.MIN_VALUE, -1L);
        testHelper(testData, (element, out) -> out.writeLong(element), DataInput::readLong);
    }

    @Test
    void readShort() throws Exception {
        List<Short> testData =
                Arrays.asList((short) 0, (short) 1, Short.MAX_VALUE, Short.MIN_VALUE, (short) -1);
        testHelper(testData, (element, out) -> out.writeShort((int) element), DataInput::readShort);
    }

    @Test
    void readUTF() throws Exception {
        List<String> testData = Arrays.asList("test", "", "hello world");
        testHelper(testData, (element, out) -> out.writeUTF(element), DataInput::readUTF);
    }

    @Test
    void readUnsignedByte() throws Exception {
        List<Integer> testData = Arrays.asList(0, 1, 127, 128, 255);
        testHelper(testData, (element, out) -> out.writeByte(element), DataInput::readUnsignedByte);
    }

    @Test
    void readUnsignedShort() throws Exception {
        List<Integer> testData = Arrays.asList(0, 1, 127, 128, 255, 256, 65534, 65535);
        testHelper(
                testData, (element, out) -> out.writeShort(element), DataInput::readUnsignedShort);
    }

    @Test
    void readLine() throws Exception {
        List<String> testData = Arrays.asList("a", "", "b and c", " ", "hello", "\rtest", "");
        testHelper(
                testData,
                (element, out) -> out.write((element + "\n").getBytes()),
                DataInput::readLine);
        testHelper(
                testData,
                (element, out) -> out.write((element + "\r\n").getBytes()),
                DataInput::readLine);
    }

    @Test
    void readFully() throws Exception {
        byte[] bytes = {1, 2, 3, 4, 5, 6};

        DataInputDeserializer inputView = prepareBulkData(out -> out.write(bytes));

        inputView.readFully(new byte[0]);
        assertEquals(0, inputView.getPosition());

        byte[] read = new byte[2];
        inputView.readFully(read);
        assertArrayEquals(new byte[] {1, 2}, read);
        assertEquals(2, inputView.getPosition());
        inputView.readFully(read);
        assertArrayEquals(new byte[] {3, 4}, read);
        assertEquals(4, inputView.getPosition());
        inputView.readFully(read);
        assertArrayEquals(new byte[] {5, 6}, read);
        assertEquals(6, inputView.getPosition());

        inputView = prepareBulkData(out -> out.write(bytes));

        read = new byte[6];
        inputView.readFully(read);
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6}, read);
        assertEquals(6, inputView.getPosition());
    }

    @Test
    void skipBytes() throws Exception {
        byte[] bytes = {1, 2, 3, 4, 5, 6};
        DataInputDeserializer inputView = prepareBulkData(out -> out.write(bytes));
        int skipped = inputView.skipBytes(3);
        assertEquals(3, skipped);
        assertEquals(3, inputView.getPosition());
        skipped = inputView.skipBytes(4);
        assertEquals(3, skipped);
        assertEquals(6, inputView.getPosition());
    }

    @Test
    void skipBytesToRead() throws Exception {
        byte[] bytes = {1, 2, 3, 4, 5, 6};
        DataInputDeserializer inputView = prepareBulkData(out -> out.write(bytes));
        inputView.skipBytesToRead(3);
        assertEquals(3, inputView.getPosition());
        inputView.skipBytesToRead(1);
        assertEquals(4, inputView.getPosition());
        try {
            inputView.skipBytesToRead(3);
            fail();
        } catch (IOException expected) {
        }
    }

    @Test
    void read() throws Exception {
        byte[] bytes = {1, 2, 3, 4, 5, 6};
        DataInputDeserializer inputView = prepareBulkData(out -> out.write(bytes));

        byte[] read = new byte[bytes.length];
        int readBytes = 0;
        while (inputView.available() > 0) {
            readBytes += inputView.read(read, readBytes, bytes.length - readBytes);
            assertEquals(readBytes, inputView.getPosition());
        }
        assertArrayEquals(bytes, read);
    }

    // ============ Helper Methods ============

    private <T> void testHelper(
            List<T> testData,
            BiConsumerWithException<T, DataOutputSerializer, Exception> writeFun,
            FunctionWithException<DataInputDeserializer, T, Exception> readFun)
            throws Exception {

        List<Integer> offsetsOut = new ArrayList<>();
        DataInputDeserializer inputView = prepareElementData(testData, writeFun, offsetsOut);
        final int bytesWritten = out.length();
        assertEquals(0, inputView.getPosition());
        assertEquals(bytesWritten, inputView.available());

        if (!testData.isEmpty()) {
            int idx = 0;
            while (idx < testData.size()) {
                int offset = offsetsOut.get(idx);
                assertEquals(testData.get(idx), readFun.apply(inputView));
                ++idx;
                assertEquals(offset, inputView.getPosition());
                assertEquals(bytesWritten - offset, inputView.available());
            }
        }
    }

    DataInputDeserializer prepareBulkData(ThrowingConsumer<DataOutputSerializer, Exception> writeTo)
            throws Exception {
        out.clear();
        writeTo.accept(out);
        return new DataInputDeserializer(out.getCopyOfBuffer());
    }

    <T> DataInputDeserializer prepareElementData(
            List<T> testData,
            BiConsumerWithException<T, DataOutputSerializer, Exception> writeFun,
            List<Integer> offsetsOut)
            throws Exception {
        out.clear();
        for (T element : testData) {
            writeFun.accept(element, out);
            offsetsOut.add(out.length());
        }
        return new DataInputDeserializer(out.getCopyOfBuffer());
    }
}
