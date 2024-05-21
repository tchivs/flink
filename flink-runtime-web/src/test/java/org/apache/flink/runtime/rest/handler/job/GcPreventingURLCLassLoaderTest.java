/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URL;
import java.util.ArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Confluent
class GcPreventingURLCLassLoaderTest {
    private static GcPreventingURLCLassLoader createClassLoader() {
        return new GcPreventingURLCLassLoader(new URL[0]);
    }

    private static Stream<Class<?>> storedClasses() {
        return Stream.of(StaticClass.class, NonStaticClass.class);
    }

    @ParameterizedTest
    @MethodSource("storedClasses")
    void testStaticClassesStored(Class<?> clazz) throws Exception {
        try (final GcPreventingURLCLassLoader cl = createClassLoader()) {
            cl.loadClass(clazz.getName());
            assertThat(cl.getAccessedClasses()).containsExactly(clazz);
        }
    }

    private static Stream<Class<?>> nonStoredClasses() {
        return Stream.of(ArrayList.class, anonymousClass);
    }

    @ParameterizedTest
    @MethodSource("nonStoredClasses")
    void testOnlyStaticNonJdkClassesStored(Class<?> clazz) throws Exception {
        try (final GcPreventingURLCLassLoader cl = createClassLoader()) {
            cl.loadClass(clazz.getName());
            assertThat(cl.getAccessedClasses()).isEmpty();
        }
    }

    @Test
    void testCloseAllowsGc() throws Exception {
        final GcPreventingURLCLassLoader cl = createClassLoader();
        try (GcPreventingURLCLassLoader ignored = cl) {
            cl.loadClass(StaticClass.class.getName());
        }
        assertThat(cl.getAccessedClasses()).isEmpty();
    }

    static Class<?> anonymousClass =
            new Runnable() {
                private final int ignored = 4;

                @Override
                public void run() {}
            }.getClass();

    public final class NonStaticClass {}

    public static final class StaticClass {}
}
