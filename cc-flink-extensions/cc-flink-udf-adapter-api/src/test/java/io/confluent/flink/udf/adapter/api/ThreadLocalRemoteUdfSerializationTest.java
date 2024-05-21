/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.api;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests ThreadLocalRemoteUdfSerialization. * */
public class ThreadLocalRemoteUdfSerializationTest {

    private static final int ITERATIONS = 10_000_000;

    private ThreadLocalRemoteUdfSerialization serialization;

    private ExecutorService executorService;

    @BeforeEach
    public void setUp() throws Throwable {
        executorService = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    public void tearDown() throws Throwable {
        executorService.shutdownNow();
    }

    @Test
    public void testSerializersSameThread() throws Throwable {
        serialization =
                new ThreadLocalRemoteUdfSerialization(
                        createSerializer(), ImmutableList.of(createSerializer()));
        sameThreadArgs(new Object[] {1});
        sameThreadArgs(new Object[] {1234});
        sameThreadReturn(1);
        sameThreadReturn(1234);
        serialization.close();

        serialization =
                new ThreadLocalRemoteUdfSerialization(
                        createSerializer(),
                        ImmutableList.of(
                                createSerializer(), createSerializer(), createSerializer()));
        sameThreadArgs(new Object[] {3, 4, 5});
        serialization.close();
    }

    @Test
    public void testMultipleThreads() throws Throwable {
        serialization =
                new ThreadLocalRemoteUdfSerialization(
                        createSerializer(), ImmutableList.of(createSerializer()));
        List<Future<?>> futures = new ArrayList<>();
        futures.add(argIterations());
        futures.add(argIterations());
        futures.add(returnIterations());
        futures.add(returnIterations());
        for (Future<?> future : futures) {
            future.get();
        }
    }

    private Future<?> argIterations() {
        return executorService.submit(
                () -> {
                    for (int i = 0; i < ITERATIONS; i++) {
                        try {
                            sameThreadArgs(new Object[] {i});
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private Future<?> returnIterations() {
        return executorService.submit(
                () -> {
                    for (int i = 0; i < ITERATIONS; i++) {
                        try {
                            sameThreadReturn(i);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private void sameThreadArgs(Object[] args) throws IOException {
        Object[] dArgs = new Object[args.length];
        serialization.deserializeArguments(
                serialization.serializeArguments(args).asReadOnlyByteBuffer(), dArgs);
        assertThat(dArgs).isEqualTo(args);
    }

    private void sameThreadReturn(Object retVal) throws IOException {
        Object result =
                serialization.deserializeReturnValue(serialization.serializeReturnValue(retVal));
        assertThat(result).isEqualTo(retVal);
    }

    @SuppressWarnings("unchecked")
    private TypeSerializer<Object> createSerializer() {
        return (TypeSerializer<Object>) (Object) new TestSerializer();
    }

    private static class TestSerializer extends TypeSerializer<Integer> {

        private static final long serialVersionUID = 1L;

        private final Thread currentThread;

        public TestSerializer() {
            this(null);
        }

        public TestSerializer(Thread currentThread) {
            this.currentThread = currentThread;
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<Integer> duplicate() {
            return new TestSerializer(Thread.currentThread());
        }

        @Override
        public Integer createInstance() {
            return 0;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            checkThread();
            StringValue.writeString(Integer.toString(record), target);
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            checkThread();
            return Integer.parseInt(StringValue.readString(source));
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
            checkThread();
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            checkThread();
            StringValue.copyString(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj.getClass().equals(this.getClass());
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return null;
        }

        private void checkThread() {
            if (currentThread != Thread.currentThread()) {
                throw new RuntimeException("Bad thread access");
            }
        }
    }
}
