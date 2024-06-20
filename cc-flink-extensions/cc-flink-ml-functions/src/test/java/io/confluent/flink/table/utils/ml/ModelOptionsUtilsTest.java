/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.ml;

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for modelOptionsUtils class. */
public class ModelOptionsUtilsTest {

    @Test
    void testGetProviderOption() {
        ModelOptionsUtils modelOptionsUtils = getModelOptionsUtils();
        assert (modelOptionsUtils
                .getProviderOption("ENDPOINT")
                .equals("https://api.openai.com/v1/chat/completions"));
        assert (modelOptionsUtils.getProviderOptionOrDefault("TEMPERATURE", "0.7").equals("0.7"));
    }

    @Test
    void testGetEncryptStrategyPlaintext() {
        ModelOptionsUtils modelOptionsUtils = getModelOptionsUtils();
        assertThat(modelOptionsUtils.getEncryptStrategy())
                .isEqualTo(EncryptionStrategy.PLAINTEXT.name());
    }

    @Test
    void testGetDefaultVersion() {
        ModelOptionsUtils modelOptionsUtils = getModelOptionsUtils();
        assertThat(modelOptionsUtils.getDefaultVersion()).isEqualTo("10");
    }

    @Test
    void testGetWithDefault() {
        ModelOptionsUtils modelOptionsUtils = getModelOptionsUtils();
        assertThat(modelOptionsUtils.getOptionOrDefault("non_exist", "defaultValue"))
                .isEqualTo("defaultValue");
    }

    @Test
    void testGetModelKindException() {
        assertThatThrownBy(() -> ModelOptionsUtils.getModelKind(Collections.emptyMap()))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "'provider' must be specified for model."));
    }

    @Test
    void testGetModelKind() {
        final MLModelCommonConstants.ModelKind modelKind =
                ModelOptionsUtils.getModelKind(Collections.singletonMap("provider", "openai"));
        assertThat(modelKind).isEqualTo(MLModelCommonConstants.ModelKind.REMOTE);
    }

    @Test
    void testGetModelTaskExceptionEmptyMap() {
        assertThatThrownBy(() -> ModelOptionsUtils.getModelTask(Collections.emptyMap()))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class, "'task' must be specified for model."));
    }

    @Test
    void testGetModelTaskExceptionInvalidTask() {
        assertThatThrownBy(
                        () ->
                                ModelOptionsUtils.getModelTask(
                                        Collections.singletonMap("task", "invalid")))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "Task value 'INVALID' is not supported. Supported values are: [REGRESSION, CLASSIFICATION, CLUSTERING, TEXT_GENERATION, EMBEDDING]"));
    }

    @ParameterizedTest
    @EnumSource(MLModelCommonConstants.ModelTask.class)
    void testGetModelTaskUpperCase(MLModelCommonConstants.ModelTask task) {
        final MLModelCommonConstants.ModelTask modelTask =
                ModelOptionsUtils.getModelTask(Collections.singletonMap("task", task.name()));
        assertThat(modelTask).isEqualTo(task);
    }

    @ParameterizedTest
    @EnumSource(MLModelCommonConstants.ModelTask.class)
    void testGetModelTaskKeyUpperCase(MLModelCommonConstants.ModelTask task) {
        final MLModelCommonConstants.ModelTask modelTask =
                ModelOptionsUtils.getModelTask(Collections.singletonMap("TASK", task.name()));
        assertThat(modelTask).isEqualTo(task);
    }

    @ParameterizedTest
    @EnumSource(MLModelCommonConstants.ModelTask.class)
    void testGetModelTaskLowerCase(MLModelCommonConstants.ModelTask task) {
        final MLModelCommonConstants.ModelTask modelTask =
                ModelOptionsUtils.getModelTask(
                        Collections.singletonMap("task", task.name().toLowerCase()));
        assertThat(modelTask).isEqualTo(task);
    }

    @Test
    void testGetInvalidModelTask() {
        assertThatThrownBy(
                        () ->
                                ModelOptionsUtils.getModelTask(
                                        Collections.singletonMap("task", "somename")))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Task value 'SOMENAME' is not supported. Supported values are: [REGRESSION, CLASSIFICATION, CLUSTERING, TEXT_GENERATION, EMBEDDING]");
    }

    @NotNull
    private static ModelOptionsUtils getModelOptionsUtils() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("OPENAI.ENDPOINT", "https://api.openai.com/v1/chat/completions");
        modelOptions.put("provider", "openai");
        modelOptions.put("task", "TEXT_GENERATION");
        modelOptions.put(
                "CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", EncryptionStrategy.PLAINTEXT.name());
        modelOptions.put("default_version", "10");

        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        return new ModelOptionsUtils(model, "OPENAI");
    }
}
