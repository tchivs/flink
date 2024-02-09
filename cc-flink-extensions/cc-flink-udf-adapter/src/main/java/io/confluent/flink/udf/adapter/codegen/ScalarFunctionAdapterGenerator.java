/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.codegen;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.generated.CompileUtils;

import io.confluent.flink.udf.adapter.ScalarFunctionInstanceCallAdapter;
import org.apache.commons.lang3.ClassUtils;

import java.util.List;

/** Utility class to generate adapter code for calling ScalarFunctions. */
public final class ScalarFunctionAdapterGenerator {

    private static final String CODE_TEMPLATE =
            "public class %s implements %s { "
                    + " @Override "
                    + " public %s call(Object instance, Object[] args) throws Exception { "
                    + " return ((%s)instance).%s(%s); "
                    + " }"
                    + "}";

    private static Class<?> invokeCompiler(
            ClassLoader classLoader, String shimClassName, String code) {
        return CompileUtils.compile(classLoader, shimClassName, code);
    }

    public static ScalarFunctionInstanceCallAdapter generate(
            String callerUUID,
            ScalarFunction instance,
            String functionName,
            Class<?> returnClass,
            List<Class<?>> paramTypes,
            ClassLoader classLoader)
            throws Exception {

        StringBuilder callArgs = new StringBuilder();
        for (int i = 0; i < paramTypes.size(); ++i) {
            Class<?> paramClass = paramTypes.get(i);
            if (callArgs.length() > 0) {
                callArgs.append(", ");
            }

            if (paramClass.isPrimitive()) {
                // The language level of CompileUtils requires this special handling of primitives.
                callArgs.append("(").append(paramClass.getCanonicalName()).append(")");
                paramClass = ClassUtils.primitiveToWrapper(paramClass);
            }

            callArgs.append("(")
                    .append(paramClass.getCanonicalName())
                    .append(")args[")
                    .append(i)
                    .append("]");
        }

        String generatedClassName =
                instance.getClass().getSimpleName() + "Adapter_" + callerUUID.replace('-', '_');
        String code =
                String.format(
                        CODE_TEMPLATE,
                        generatedClassName,
                        ScalarFunctionCallAdapter.class.getCanonicalName(),
                        returnClass.getCanonicalName(),
                        instance.getClass().getCanonicalName(),
                        functionName,
                        callArgs);

        Class<?> generatedClass = invokeCompiler(classLoader, generatedClassName, code);
        ScalarFunctionCallAdapter scalarFunctionCallAdapter =
                (ScalarFunctionCallAdapter) generatedClass.getDeclaredConstructor().newInstance();
        return new ScalarFunctionInstanceCallAdapter(instance, scalarFunctionCallAdapter);
    }
}
