/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkUserCodeClassLoader;

import java.util.Optional;

final class TypeFailureEnricherUtils {
    /**
     * @param t the Throwable to check
     * @return true when the Throwable is a User Secret error message {@code
     *     io.confluent.flink.table.modules.ai.AISecret}, false otherwise
     */
    @Internal
    public static boolean isUserSecretErrorMessage(Throwable t) {
        return ExceptionUtils.findThrowableWithMessage(t, "SECRET is null.").isPresent();
    }

    /**
     * @param classLoader the clasLoader to check
     * @return true when class is loaded from user artifacts, false otherwise
     */
    @Internal
    public static boolean isUserCodeClassLoader(ClassLoader classLoader) {
        return classLoader instanceof FlinkUserCodeClassLoader;
    }

    /**
     * Use reflection to return the class on the top of the Throwable stackTrace by calling
     * Class.forName with the provided classLoader.
     *
     * @param throwable
     * @param classLoader
     * @return Optionally the class at the top of the stackTrace
     */
    public static Optional<Class<?>> findClassFromStackTraceTop(
            Throwable throwable, ClassLoader classLoader) {
        for (StackTraceElement currElement : throwable.getStackTrace()) {
            try {
                Class<?> topClass = Class.forName(currElement.getClassName(), false, classLoader);
                return Optional.of(topClass);
            } catch (ClassNotFoundException ex) {
                // continue
            }
        }
        return Optional.empty();
    }

    /**
     * Checks whether a throwable chain contains a specific type of exception and returns it.
     *
     * <p>Unlike {@link ExceptionUtils#findThrowable(Throwable, Class)} this method checks by class
     * name and not by class object. This is intended to be used when the Throwable is loaded by a
     * different classloader than the class object.
     *
     * @param throwable the throwable chain to check.
     * @param searchType the type of exception to search for in the chain.
     * @return Optional throwable of the requested type if available, otherwise empty
     */
    public static Optional<Throwable> findThrowableByName(
            Throwable throwable, Class<?> searchType) {
        if (searchType == null) {
            return Optional.empty();
        }
        final String searchName = searchType.getCanonicalName();
        return ExceptionUtils.findThrowable(
                throwable, (t) -> t.getClass().getName().contains(searchName));
    }
}
