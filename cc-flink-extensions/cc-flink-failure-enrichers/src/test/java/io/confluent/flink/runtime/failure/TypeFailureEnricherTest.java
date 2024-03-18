/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.SerializedThrowable;

import io.confluent.flink.table.modules.ai.AISecret;
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
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static io.confluent.flink.runtime.failure.TypeFailureEnricherTableITCase.assertFailureEnricherLabelIsExpectedLabel;
import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertFalse(TypeFailureEnricherUtils.isUserCodeClassLoader(systemClass.getClassLoader()));

        Class<?> userClass = Class.forName(USER_CLASS, false, classLoader);
        assertTrue(TypeFailureEnricherUtils.isUserCodeClassLoader(userClass.getClassLoader()));
    }

    @Test
    void testTypeFailureEnricherCases() throws ExecutionException, InterruptedException {
        assertFailureEnricherLabelIsExpectedLabel(
                new SerializedThrowable(new Exception("serialization error")),
                Arrays.asList("JOB_CANNOT_RESTART", "ERROR_CLASS_CODE"),
                "USER",
                "1");
        assertFailureEnricherLabelIsExpectedLabel(
                new ArithmeticException("test"),
                Arrays.asList("JOB_CANNOT_RESTART", "ERROR_CLASS_CODE"),
                "USER",
                "5");
        assertFailureEnricherLabelIsExpectedLabel(
                new NumberFormatException("test"),
                Collections.singletonList("ERROR_CLASS_CODE"),
                "USER",
                "10");
        assertFailureEnricherLabelIsExpectedLabel(
                new DateTimeException("test"),
                Collections.singletonList("ERROR_CLASS_CODE"),
                "USER",
                "11");
        assertFailureEnricherLabelIsExpectedLabel(
                new TransactionalIdAuthorizationException("test"),
                Collections.singletonList("ERROR_CLASS_CODE"),
                "USER",
                "12");
        assertFailureEnricherLabelIsExpectedLabel(
                new TopicAuthorizationException("test"),
                Collections.singletonList("ERROR_CLASS_CODE"),
                "USER",
                "13");
        assertFailureEnricherLabelIsExpectedLabel(
                new FlinkException("test"),
                Collections.singletonList("ERROR_CLASS_CODE"),
                "SYSTEM",
                "15");
        assertFailureEnricherLabelIsExpectedLabel(
                new ExpectedTestException("test"),
                Collections.singletonList("ERROR_CLASS_CODE"),
                "UNKNOWN",
                "0");
    }

    @Test
    void testTableExceptionClassificationNullValue()
            throws ExecutionException, InterruptedException {
        final String errorMsg =
                "Column 'b' is NOT NULL, however, a null value is being written into it. "
                        + "You can set job configuration 'table.exec.sink.not-null-enforcer'='DROP' "
                        + "to suppress this exception and drop such records silently.";
        assertFailureEnricherLabelIsExpectedLabel(
                new TableException(errorMsg),
                Arrays.asList("JOB_CANNOT_RESTART", "ERROR_CLASS_CODE"),
                "USER",
                "4");
    }

    @Test
    void testTableExceptionClassification() throws ExecutionException, InterruptedException {
        final String errorMsg = "Some other error message";
        assertFailureEnricherLabelIsExpectedLabel(
                new TableException(errorMsg),
                Collections.singletonList("ERROR_CLASS_CODE"),
                "USER",
                "3");
    }

    @Test
    void testUserSecretExceptionClassification() throws ExecutionException, InterruptedException {
        Exception toValidate =
                new FlinkRuntimeException(String.format(AISecret.ERROR_MESSAGE, "name"));
        assertFailureEnricherLabelIsExpectedLabel(
                toValidate, Collections.singletonList("ERROR_CLASS_CODE"), "USER", "14");
    }

    @Test
    void testShadedDepsClassification() {
        assertTrue(
                TypeFailureEnricherUtils.findThrowableByName(
                                new io.confluent.shaded.io.confluent.flink.runtime.failure.mock
                                        .MockedException(),
                                io.confluent.flink.runtime.failure.mock.MockedException.class)
                        .isPresent());
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
        JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jarFile.toPath()));
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
