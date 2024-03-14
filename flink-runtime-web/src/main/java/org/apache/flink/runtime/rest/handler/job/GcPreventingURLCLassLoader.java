/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Confluent
class GcPreventingURLCLassLoader extends URLClassLoader {
    static {
        registerAsParallelCapable();
    }

    private final Set<Class<?>> accessedClasses = Collections.synchronizedSet(new HashSet<>());

    public GcPreventingURLCLassLoader(URL[] urls) {
        super(urls);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> loadedClass = super.loadClass(name, resolve);
        // exclude anonymous/lambda/jdk classes
        if (!loadedClass.isAnonymousClass()
                && !loadedClass.isSynthetic()
                && !name.startsWith("java.")) {
            accessedClasses.add(loadedClass);
        }
        return loadedClass;
    }

    @Override
    public void close() throws IOException {
        accessedClasses.clear();
        super.close();
    }

    @VisibleForTesting
    Set<Class<?>> getAccessedClasses() {
        return accessedClasses;
    }
}
