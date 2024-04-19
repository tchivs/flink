/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.exceptions.ModelNotExistException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static io.confluent.flink.table.modules.ml.RemoteModelValidator.validateCreateModelOptions;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link RemoteModelValidator}. */
public class RemoteModelValidatorTest {

    private static TableEnvironment tableEnv;

    @BeforeAll
    static void beforeAll() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    void testInvalidProviderOptions() {
        testCreateModelError(
                "CREATE MODEL t WITH ("
                        + "'provider' = 'unknown',"
                        + "'task' = 'text-generation',"
                        + "'unknown.provider' = 'random')",
                "Unsupported 'PROVIDER': unknown");
    }

    @Test
    void testMissingProviderOptions() {
        testCreateModelError(
                "CREATE MODEL t WITH (" + "'task' = 'text-generation')", "'PROVIDER' is not set");
    }

    @Test
    void testMissingTopLevelOptions() {
        testCreateModelError(
                "CREATE MODEL t WITH (" + "'provider' = 'openai'," + "'openai.api_key' = 'key')",
                "One or more required options are missing.");
    }

    @Test
    void testMissingProviderLevelOptions() {
        testCreateModelError(
                "CREATE MODEL t WITH ("
                        + "'task' = 'text_generation',"
                        + "'provider' = 'bedrock',"
                        + "'bedrock.endpoint' = 'endpoint',"
                        + "'bedrock.aws_access_key_id' = 'key_id')",
                "BEDROCK.AWS_SECRET_ACCESS_KEY");
    }

    @Test
    void testExtraTopLevelOptions() {
        testCreateModelError(
                "CREATE MODEL t WITH ("
                        + "'PROVIDER' = 'openai',"
                        + "'TASK' = 'text_generation',"
                        + "'OPENAI.api_key' = 'key',"
                        + "'system_prompt' = 'count prime numbers',"
                        + "'proxy' = 'proxy')",
                "Unsupported options:" + "\n" + "\n" + "PROXY");
    }

    @Test
    void testExtraProviderLevelOption() {
        testCreateModelError(
                "CREATE MODEL t WITH ("
                        + "'PROVIDER' = 'vertexai',"
                        + "'TASK' = 'text_generation',"
                        + "'vertexai.service_key' = 'key',"
                        + "'vertexai.proxy' = 'proxy')",
                "Unsupported options:" + "\n" + "\n" + "VERTEXAI.PROXY");
    }

    @Test
    void testAllowParamsLevelOption() {
        Map<String, String> options =
                testCreateModelOptions(
                        "CREATE MODEL t WITH ("
                                + "'PROVIDER' = 'vertexai',"
                                + "'TASK' = 'text_generation',"
                                + "'vertexai.service_key' = 'key',"
                                + "'vertexai.params.temp' = '0.7')");
        assertThat(options).containsEntry("VERTEXAI.PARAMS.TEMP", "0.7");
    }

    @Test
    void testNotAllowConfluentOption() {
        testCreateModelError(
                "CREATE MODEL t WITH ("
                        + "'PROVIDER' = 'vertexai',"
                        + "'TASK' = 'text_generation',"
                        + "'CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY' = 'kms',"
                        + "'vertexai.service_key' = 'key')",
                "Unsupported options:" + "\n" + "\n" + "CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY");
    }

    @Test
    void testNotAllowedParamsLevelOption() {
        testCreateModelError(
                "CREATE MODEL t WITH ("
                        + "'PROVIDER' = 'vertexai',"
                        + "'TASK' = 'text_generation',"
                        + "'vertexai.service_key' = 'key',"
                        + "'vertexai.parameters.temp' = '0.7')",
                "Unsupported options:" + "\n" + "\n" + "VERTEXAI.PARAMETERS.TEMP");
    }

    @Test
    void testAzureOpenAIOption() {
        testCreateModelError(
                "CREATE MODEL t WITH ("
                        + "'provider' = 'azureopenai',"
                        + "'taSk' = 'text_generation',"
                        + "'azureopenai.api_key' = 'key')",
                "Missing required options are:" + "\n" + "\n" + "AZUREOPENAI.ENDPOINT");
    }

    @Test
    void testGetPublicOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "openai",
                        "task", "text_generation",
                        "openai.api_key", "key",
                        "openai.endpoint", "endpoint",
                        "openai.system_prompt", "count prime numbers",
                        "confluent.model.secret.encrypt_strategy", "plaintext");
        Map<String, String> publicOptions = RemoteModelValidator.getPublicOptions(options);
        assertThat(publicOptions)
                .doesNotContainEntry("CONFLUETN.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
    }

    @Test
    void testGetPrivateOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "sagemaker",
                        "task", "text_generation",
                        "sagemaker.aws_access_key_id", "key_id",
                        "sagemaker.aws_secret_access_key", "secret_key",
                        "sagemaker.endpoint", "endpoint",
                        "sagemaker.aws_session_token", "session_token",
                        "confluent.model.secret.encrypt_strategy", "plaintext");
        Map<String, String> privateOptions = RemoteModelValidator.getPrivateOptions(options);
        assertThat(privateOptions)
                .containsOnly(entry("confluent.model.secret.encrypt_strategy", "plaintext"));
    }

    @Test
    void testCaseInsentiveOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "pRoVidEr", "sagemaker",
                        "task", "text_generation",
                        "sAgEmaKer.Aws_ACCESS_keY_id", "key_id",
                        "sagemaker.aws_secret_access_key", "secret_key",
                        "sagemaker.endpoint", "endpoint",
                        "coNflUent.ModeL.seCrEt.enCryPt_strAtegy", "plaintext");
        Map<String, String> publicOptions = RemoteModelValidator.getPublicOptions(options);
        assertThat(publicOptions).containsEntry("pRoVidEr", "sagemaker");
        Map<String, String> privateOptions = RemoteModelValidator.getPrivateOptions(options);
        assertThat(privateOptions)
                .containsEntry("coNflUent.ModeL.seCrEt.enCryPt_strAtegy", "plaintext");
        Map<String, String> secretOptions = RemoteModelValidator.getSecretOptions(options);
        assertThat(secretOptions).containsEntry("sAgEmaKer.Aws_ACCESS_keY_id", "key_id");
    }

    @Test
    void testGetSecretOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "sagemaker",
                        "task", "text_generation",
                        "sagemaker.aws_access_key_id", "key_id",
                        "sagemaker.aws_secret_access_key", "secret_key",
                        "sagemaker.endpoint", "endpoint",
                        "sagemaker.aws_session_token", "session_token",
                        "confluent.model.secret.encrypt_strategy", "plaintext");
        Map<String, String> secretOptions = RemoteModelValidator.getSecretOptions(options);
        assertThat(secretOptions)
                .containsOnly(
                        entry("sagemaker.aws_access_key_id", "key_id"),
                        entry("sagemaker.aws_secret_access_key", "secret_key"),
                        entry("sagemaker.aws_session_token", "session_token"));
    }

    @Test
    void testGetAllSecretKeys() {
        Set<String> keys = RemoteModelValidator.getAllSecretOptionsKeys();
        Set<String> expected =
                ImmutableSet.of(
                        AzureMLRemoteModelOptions.API_KEY.key(),
                        AzureOpenAIRemoteModelOptions.API_KEY.key(),
                        BedrockRemoteModelOptions.ACCESS_KEY_ID.key(),
                        BedrockRemoteModelOptions.SECRET_KEY.key(),
                        BedrockRemoteModelOptions.SESSION_TOKEN.key(),
                        GoogleAIRemoteModelOptions.API_KEY.key(),
                        OpenAIRemoteModelOptions.API_KEY.key(),
                        SageMakerRemoteModelOptions.ACCESS_KEY_ID.key(),
                        SageMakerRemoteModelOptions.SECRET_KEY.key(),
                        SageMakerRemoteModelOptions.SESSION_TOKEN.key(),
                        VertexAIRemoteModelOptions.SERVICE_KEY.key());
        assertThat(keys).isEqualTo(expected);
    }

    private void testCreateModelError(String sql, String error) {
        assertThatThrownBy(() -> testCreateModelOptions(sql)).hasMessageContaining(error);
    }

    private Map<String, String> testCreateModelOptions(String sql) {
        try {
            tableEnv.executeSql(sql);
            final ResolvedCatalogModel catalogModel =
                    (ResolvedCatalogModel)
                            tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                                    .orElseThrow(IllegalArgumentException::new)
                                    .getModel(new ObjectPath(tableEnv.getCurrentDatabase(), "t"));
            return validateCreateModelOptions("t", catalogModel.getOptions());
        } catch (ModelNotExistException e) {
            return fail(e);
        } finally {
            tableEnv.executeSql("DROP MODEL IF EXISTS t");
        }
    }
}
