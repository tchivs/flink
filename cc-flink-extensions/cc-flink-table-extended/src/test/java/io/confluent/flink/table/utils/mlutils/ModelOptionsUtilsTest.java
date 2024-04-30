/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.mlutils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogModel.ModelTask;

import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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

    @NotNull
    private static ModelOptionsUtils getModelOptionsUtils() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("OPENAI.ENDPOINT", "https://api.openai.com/v1/chat/completions");
        modelOptions.put("provider", "openai");
        modelOptions.put("task", ModelTask.TEXT_GENERATION.name());
        modelOptions.put(
                "CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", EncryptionStrategy.PLAINTEXT.name());
        modelOptions.put("default_version", "10");

        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        return new ModelOptionsUtils(model, "OPENAI");
    }
}
