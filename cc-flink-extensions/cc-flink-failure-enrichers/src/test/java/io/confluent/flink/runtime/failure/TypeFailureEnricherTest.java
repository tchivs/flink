/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.SerializedThrowable;

import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static io.confluent.flink.runtime.failure.TypeFailureEnricherTableITCase.assertFailureEnricherLabelIsExpectedLabel;
import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class TypeFailureEnricherTest {
    @TempDir static File temporaryFile;

    public static final String USER_CLASS = "UserClass";
    public static final String USER_CLASS_CODE =
            "import java.io.Serializable;\n"
                    + "public class "
                    + USER_CLASS
                    + " implements Serializable {}";

    private static File userJar;

    @BeforeAll
    public static void prepare() throws Exception {
        userJar =
                createJarFile(
                        temporaryFile,
                        "test-classloader.jar",
                        Collections.singletonMap(USER_CLASS, USER_CLASS_CODE));
    }

    private static MutableURLClassLoader createChildFirstClassLoader(
            URL childCodePath, ClassLoader parentClassLoader) {
        return FlinkUserCodeClassLoaders.childFirst(
                new URL[] {childCodePath},
                parentClassLoader,
                new String[0],
                NOOP_EXCEPTION_HANDLER,
                true);
    }

    @Test
    void testIsUserCodeClassLoader() throws Exception {
        // collect the libraries / class folders with RocksDB related code: the state backend and
        // RocksDB itself
        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();
        final MutableURLClassLoader classLoader =
                createChildFirstClassLoader(childCodePath, getClass().getClassLoader());
        classLoader.addURL(userJar.toURI().toURL());

        final Class<?> systemClass =
                Class.forName(FailureEnricher.class.getName(), false, classLoader);
        assertEquals(
                false,
                TypeFailureEnricherUtils.isUserCodeClassLoader(systemClass.getClassLoader()));

        Class<?> userClass = Class.forName(USER_CLASS, false, classLoader);
        assertTrue(TypeFailureEnricherUtils.isUserCodeClassLoader(userClass.getClassLoader()));
    }

    @Test
    void testTypeFailureEnricherCases() throws ExecutionException, InterruptedException {
        assertFailureEnricherLabelIsExpectedLabel(
                new SerializedThrowable(new Exception("serialization error")), "USER");
        assertFailureEnricherLabelIsExpectedLabel(new ArithmeticException("test"), "USER");
        assertFailureEnricherLabelIsExpectedLabel(new NumberFormatException("test"), "USER");
        assertFailureEnricherLabelIsExpectedLabel(new DateTimeException("test"), "USER");
        assertFailureEnricherLabelIsExpectedLabel(
                new TransactionalIdAuthorizationException("test"), "USER");
        assertFailureEnricherLabelIsExpectedLabel(new TopicAuthorizationException("test"), "USER");
        assertFailureEnricherLabelIsExpectedLabel(new FlinkException("test"), "SYSTEM");
        assertFailureEnricherLabelIsExpectedLabel(new ExpectedTestException("test"), "UNKNOWN");
    }

    /** Pack the generated classes into a JAR and return the path of the JAR. */
    public static File createJarFile(
            File tmpDir, String jarName, Map<String, String> classNameCodes) throws IOException {
        List<File> javaFiles = new ArrayList<>();
        for (Map.Entry<String, String> entry : classNameCodes.entrySet()) {
            // write class source code to file
            File javaFile = Paths.get(tmpDir.toString(), entry.getKey() + ".java").toFile();
            //noinspection ResultOfMethodCallIgnored
            javaFile.createNewFile();
            FileUtils.writeFileUtf8(javaFile, entry.getValue());

            javaFiles.add(javaFile);
        }

        // compile class source code
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(diagnostics, null, null);
        Iterable<? extends JavaFileObject> compilationUnit =
                fileManager.getJavaFileObjectsFromFiles(javaFiles);
        JavaCompiler.CompilationTask task =
                compiler.getTask(
                        null,
                        fileManager,
                        diagnostics,
                        Collections.emptyList(),
                        null,
                        compilationUnit);
        task.call();

        // pack class file to jar
        File jarFile = Paths.get(tmpDir.toString(), jarName).toFile();
        JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile));
        for (String className : classNameCodes.keySet()) {
            File classFile = Paths.get(tmpDir.toString(), className + ".class").toFile();
            JarEntry jarEntry = new JarEntry(className + ".class");
            jos.putNextEntry(jarEntry);
            byte[] classBytes = FileUtils.readAllBytes(classFile.toPath());
            jos.write(classBytes);
            jos.closeEntry();
        }
        jos.close();

        return jarFile;
    }
}
