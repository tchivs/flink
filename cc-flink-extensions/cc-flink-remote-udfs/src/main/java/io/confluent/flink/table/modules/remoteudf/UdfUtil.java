/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/** A utility with methods common to Udf classes. */
public class UdfUtil {
    private static final Logger LOG = LoggerFactory.getLogger(UdfUtil.class);

    public static final String FUNCTIONS_PREFIX = "confluent.functions.";
    public static final String FUNCTION_NAME_FIELD = "name";
    public static final String FUNCTION_DATABASE_FIELD = "database";
    public static final String FUNCTION_CATALOG_FIELD = "catalog";
    public static final String FUNCTION_ARGUMENT_TYPES_FIELD = "argumentTypes";
    public static final String FUNCTION_RETURN_TYPE_FIELD = "returnType";
    public static final String FUNCTION_ID_FIELD = "id";
    public static final String FUNCTION_CLASS_NAME_FIELD = "className";

    private static final List<Field> ALL_FIELDS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            Field.of(FUNCTION_NAME_FIELD, ConfiguredFunctionSpec.Builder::setName),
                            Field.of(
                                    FUNCTION_DATABASE_FIELD,
                                    ConfiguredFunctionSpec.Builder::setDatabase),
                            Field.of(
                                    FUNCTION_CATALOG_FIELD,
                                    ConfiguredFunctionSpec.Builder::setCatalog),
                            Field.ofVariable(
                                    FUNCTION_ARGUMENT_TYPES_FIELD,
                                    ConfiguredFunctionSpec.Builder::addArgumentTypes),
                            Field.ofVariable(
                                    FUNCTION_RETURN_TYPE_FIELD,
                                    ConfiguredFunctionSpec.Builder::addReturnType),
                            Field.of(
                                    FUNCTION_ID_FIELD,
                                    ConfiguredFunctionSpec.Builder::setFunctionId),
                            Field.of(
                                    FUNCTION_CLASS_NAME_FIELD,
                                    ConfiguredFunctionSpec.Builder::setClassName)));

    public static List<ConfiguredRemoteScalarFunction> extractUdfs(Map<String, String> config) {
        final Set<String> udfNames = extractUdfNames(config);
        final List<ConfiguredRemoteScalarFunction> udfs = new ArrayList<>();
        for (String udfName : udfNames) {
            List<ConfiguredFunctionSpec> extractedSpecs = extractSpecs(config, udfName);
            udfs.add(new ConfiguredRemoteScalarFunction(config, extractedSpecs));
        }
        return udfs;
    }

    private static Set<String> extractUdfNames(Map<String, String> config) {
        final Set<String> names = new HashSet<>();
        for (String key : config.keySet()) {
            if (key.startsWith(FUNCTIONS_PREFIX)) {
                int nextDotIndex = key.indexOf(".", FUNCTIONS_PREFIX.length());
                if (nextDotIndex < 0) {
                    LOG.error("Badly formed config " + key);
                    throw new FlinkRuntimeException("Badly formed config " + key);
                }
                String name = key.substring(FUNCTIONS_PREFIX.length(), nextDotIndex);
                names.add(name);
            }
        }
        return names;
    }

    private static List<ConfiguredFunctionSpec> extractSpecs(
            Map<String, String> config, String udfName) {
        ConfiguredFunctionSpec.Builder builder = ConfiguredFunctionSpec.newBuilder();
        for (Field field : ALL_FIELDS) {
            boolean emptyField = true;
            if (field.variable) {
                for (int i = 0; ; i++) {
                    String fieldValue =
                            config.get(FUNCTIONS_PREFIX + udfName + "." + field.name + "." + i);
                    if (fieldValue == null) {
                        break;
                    }
                    field.consumer.accept(builder, fieldValue);
                    emptyField = false;
                }

            } else {
                String fieldValue = config.get(FUNCTIONS_PREFIX + udfName + "." + field.name);
                if (!Strings.isNullOrEmpty(fieldValue)) {
                    field.consumer.accept(builder, fieldValue);
                    emptyField = false;
                }
            }
            if (emptyField) {
                LOG.error("Didn't find field " + field.name + " for udf " + udfName);
                throw new FlinkRuntimeException(
                        "Didn't find field " + field.name + " for udf " + udfName);
            }
        }
        return builder.build();
    }

    public static TypeInference getTypeInference(
            List<DataType> argumentTypes, DataType returnType) {
        TypeInference.Builder builder = TypeInference.newBuilder();
        builder.typedArguments(
                argumentTypes.stream().map(DataType::toInternal).collect(Collectors.toList()));
        builder.outputTypeStrategy(TypeStrategies.explicit(returnType.toInternal()));
        return builder.build();
    }

    private static class Field {

        final String name;
        final BiConsumer<ConfiguredFunctionSpec.Builder, String> consumer;
        final boolean variable;

        public Field(
                String name,
                BiConsumer<ConfiguredFunctionSpec.Builder, String> consumer,
                boolean variable) {
            this.name = name;
            this.consumer = consumer;
            this.variable = variable;
        }

        public static Field of(
                String name, BiConsumer<ConfiguredFunctionSpec.Builder, String> consumer) {
            return new Field(name, consumer, false);
        }

        public static Field ofVariable(
                String name, BiConsumer<ConfiguredFunctionSpec.Builder, String> variableConsumer) {
            return new Field(name, variableConsumer, true);
        }
    }
}
