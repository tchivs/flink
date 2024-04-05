/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import io.confluent.flink.apiserver.client.model.ApisMetaV1ObjectMeta;
import io.confluent.flink.apiserver.client.model.ComputeV1alphaArtifact;
import io.confluent.flink.apiserver.client.model.ComputeV1alphaEntryPoint;
import io.confluent.flink.apiserver.client.model.ComputeV1alphaFlinkUdfTask;
import io.confluent.flink.apiserver.client.model.ComputeV1alphaFlinkUdfTaskSpec;
import io.confluent.flink.udf.adapter.api.RemoteUdfSerialization;
import io.confluent.flink.udf.adapter.api.RemoteUdfSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID;

/** A utility with methods common to Udf classes. */
public class UdfUtil {
    private static final Logger LOG = LoggerFactory.getLogger(UdfUtil.class);

    public static final String FUNCTIONS_PREFIX = "confluent.functions.";
    public static final String FUNCTION_ORG_FIELD = "org";
    public static final String FUNCTION_ENV_FIELD = "env";
    public static final String FUNCTION_NAME_FIELD = "name";
    public static final String FUNCTION_DATABASE_FIELD = "database";
    public static final String FUNCTION_CATALOG_FIELD = "catalog";
    public static final String FUNCTION_ARGUMENT_TYPES_FIELD = "argumentTypes";
    public static final String FUNCTION_RETURN_TYPE_FIELD = "returnType";
    public static final String PLUGIN_ID_FIELD = "pluginId";
    public static final String PLUGIN_VERSION_ID_FIELD = "pluginVersionId";
    public static final String FUNCTION_CLASS_NAME_FIELD = "className";
    public static final String FUNCTION_IS_DETERMINISTIC_FIELD = "isDeterministic";

    private static final List<Field> ALL_FIELDS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            Field.of(
                                    FUNCTION_ORG_FIELD,
                                    ConfiguredFunctionSpec.Builder::setOrganization,
                                    ConfiguredFunctionSpec::getOrganization),
                            Field.of(
                                    FUNCTION_ENV_FIELD,
                                    ConfiguredFunctionSpec.Builder::setEnvironment,
                                    ConfiguredFunctionSpec::getEnvironment),
                            Field.of(
                                    FUNCTION_NAME_FIELD,
                                    ConfiguredFunctionSpec.Builder::setName,
                                    ConfiguredFunctionSpec::getName),
                            Field.of(
                                    FUNCTION_DATABASE_FIELD,
                                    ConfiguredFunctionSpec.Builder::setDatabase,
                                    ConfiguredFunctionSpec::getDatabase),
                            Field.of(
                                    FUNCTION_CATALOG_FIELD,
                                    ConfiguredFunctionSpec.Builder::setCatalog,
                                    ConfiguredFunctionSpec::getCatalog),
                            Field.ofVariable(
                                    FUNCTION_ARGUMENT_TYPES_FIELD,
                                    ConfiguredFunctionSpec.Builder::addArgumentTypes,
                                    ConfiguredFunctionSpec::getArgumentTypes,
                                    true),
                            Field.ofVariable(
                                    FUNCTION_RETURN_TYPE_FIELD,
                                    ConfiguredFunctionSpec.Builder::addReturnType,
                                    ConfiguredFunctionSpec::getReturnType,
                                    false),
                            Field.of(
                                    PLUGIN_ID_FIELD,
                                    ConfiguredFunctionSpec.Builder::setPluginId,
                                    ConfiguredFunctionSpec::getPluginId),
                            Field.of(
                                    PLUGIN_VERSION_ID_FIELD,
                                    ConfiguredFunctionSpec.Builder::setPluginVersionId,
                                    ConfiguredFunctionSpec::getPluginVersionId),
                            Field.of(
                                    FUNCTION_CLASS_NAME_FIELD,
                                    ConfiguredFunctionSpec.Builder::setClassName,
                                    ConfiguredFunctionSpec::getClassName),
                            Field.ofOptional(
                                    FUNCTION_IS_DETERMINISTIC_FIELD,
                                    ConfiguredFunctionSpec.Builder::parseIsDeterministic,
                                    ConfiguredFunctionSpec::getIsDeterministicAsString)));

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
                    if (fieldValue == null || (!field.emptyOk && fieldValue.isEmpty())) {
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
            if (!field.optional && emptyField) {
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

    @VisibleForTesting
    static String createUdfName(String catalog, String database, String name) {
        return "udf-"
                + Stream.of(catalog, database, name)
                        .map(str -> Integer.toHexString(str.hashCode()))
                        .collect(Collectors.joining("-"));
    }

    public static Map<String, String> toConfiguration(ConfiguredRemoteScalarFunction function) {
        String udfName =
                createUdfName(
                        function.getFunctionCatalog(),
                        function.getFunctionDatabase(),
                        function.getFunctionName());
        Map<String, String> config = new HashMap<>();
        int index = 0;
        for (ConfiguredFunctionSpec spec : function.getConfiguredFunctionSpecs()) {
            for (Field field : ALL_FIELDS) {
                if (field.variable) {
                    config.put(
                            FUNCTIONS_PREFIX + udfName + "." + field.name + "." + index,
                            field.getField.apply(spec));
                } else {
                    config.put(
                            FUNCTIONS_PREFIX + udfName + "." + field.name,
                            field.getField.apply(spec));
                }
            }
            index++;
        }
        return config;
    }

    static ComputeV1alphaFlinkUdfTask getUdfTaskFromSpec(
            Map<String, String> config,
            RemoteUdfSpec remoteUdfSpec,
            RemoteUdfSerialization remoteUdfSerialization)
            throws IOException {
        String pluginId = config.getOrDefault(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID.key(), "");
        String versionId = config.getOrDefault(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID.key(), "");
        Preconditions.checkArgument(!pluginId.isEmpty(), "PluginId must be set");
        Preconditions.checkArgument(!versionId.isEmpty(), "VersionId must be set");

        String udfTaskName = "udf-tm-" + UUID.randomUUID();
        ComputeV1alphaFlinkUdfTask udfTask = new ComputeV1alphaFlinkUdfTask();
        ComputeV1alphaFlinkUdfTaskSpec udfTaskSpec = new ComputeV1alphaFlinkUdfTaskSpec();

        // Metadata must contain Org, Env, and Name
        ApisMetaV1ObjectMeta udfTaskMeta = new ApisMetaV1ObjectMeta();
        udfTaskMeta.setName(udfTaskName);
        udfTaskMeta.setOrg(remoteUdfSpec.getOrganization());
        udfTaskMeta.setEnvironment(remoteUdfSpec.getEnvironment());

        // Entrypoint must contain className, open and close Payloads
        ComputeV1alphaEntryPoint udfTaskEntryPoint = new ComputeV1alphaEntryPoint();
        udfTaskEntryPoint.setClassName("io.confluent.flink.udf.adapter.ScalarFunctionHandler");
        udfTaskEntryPoint.setOpenPayload(
                remoteUdfSerialization
                        .serializeRemoteUdfSpec(remoteUdfSpec)
                        .toByteArray()); // to be removed
        // Unused at the moment -- pass something until platform supports empty payloads
        udfTaskEntryPoint.setClosePayload(Base64.getEncoder().encode("bye".getBytes()));

        // Artifact must contain name, scope, pluginId, versionId, inClassPath, and orgOpts
        ComputeV1alphaArtifact udfTaskArtifact = new ComputeV1alphaArtifact();
        udfTaskArtifact.setName("udf-task");
        udfTaskArtifact.setScope(ComputeV1alphaArtifact.ScopeEnum.ORG);
        udfTaskArtifact.setPluginId(remoteUdfSpec.getPluginId());
        udfTaskArtifact.setVersionId(remoteUdfSpec.getPluginVersionId());
        udfTaskArtifact.setInClassPath(true);

        ComputeV1alphaArtifact udfTaskArtifactInternal = new ComputeV1alphaArtifact();
        udfTaskArtifactInternal.setName("udf-shim-internal");
        udfTaskArtifactInternal.setScope(ComputeV1alphaArtifact.ScopeEnum.INTERNAL);
        udfTaskArtifactInternal.setPluginId(pluginId);
        udfTaskArtifactInternal.setVersionId(versionId);
        udfTaskArtifactInternal.setInClassPath(true);

        // Enrich UdfTaskSpec with Artifact and EntryPoint details
        udfTaskSpec.addArtifactsItem(udfTaskArtifact);
        udfTaskSpec.addArtifactsItem(udfTaskArtifactInternal);
        udfTaskSpec.setEntryPoint(udfTaskEntryPoint);
        // Assign the UdfTaskSpec to the UdfTask
        udfTask.setSpec(udfTaskSpec);
        // Assign the Metadata to the UdfTask
        udfTask.setMetadata(udfTaskMeta);
        return udfTask;
    }

    private static class Field {

        final String name;
        final BiConsumer<ConfiguredFunctionSpec.Builder, String> consumer;
        final Function<ConfiguredFunctionSpec, String> getField;
        final boolean variable;
        final boolean emptyOk;
        private final boolean optional;

        public Field(
                String name,
                BiConsumer<ConfiguredFunctionSpec.Builder, String> consumer,
                Function<ConfiguredFunctionSpec, String> getField,
                boolean variable,
                boolean emptyOk,
                boolean optional) {
            this.name = name;
            this.consumer = consumer;
            this.getField = getField;
            this.variable = variable;
            this.emptyOk = emptyOk;
            this.optional = optional;
        }

        public static Field of(
                String name,
                BiConsumer<ConfiguredFunctionSpec.Builder, String> consumer,
                Function<ConfiguredFunctionSpec, String> getField) {
            return new Field(name, consumer, getField, false, false, false);
        }

        public static Field ofOptional(
                String name,
                BiConsumer<ConfiguredFunctionSpec.Builder, String> consumer,
                Function<ConfiguredFunctionSpec, String> getField) {
            return new Field(name, consumer, getField, false, false, true);
        }

        public static Field ofVariable(
                String name,
                BiConsumer<ConfiguredFunctionSpec.Builder, String> variableConsumer,
                Function<ConfiguredFunctionSpec, String> getField,
                boolean emptyOk) {
            return new Field(name, variableConsumer, getField, true, emptyOk, false);
        }
    }
}
