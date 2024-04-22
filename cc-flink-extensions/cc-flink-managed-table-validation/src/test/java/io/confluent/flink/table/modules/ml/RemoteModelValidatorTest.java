/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static io.confluent.flink.table.modules.ml.RemoteModelValidator.validateCreateModelOptions;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** Tests for {@link RemoteModelValidator}. */
public class RemoteModelValidatorTest {

    @Test
    void testInvalidProviderOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "unknown",
                        "task", "text_generation",
                        "unknown.api_key", "key");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining("Unsupported 'PROVIDER': unknown");
    }

    @Test
    void testMissingProviderOptions() {
        Map<String, String> options = ImmutableMap.of("task", "text_generation");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining("'PROVIDER' is not set");
    }

    @Test
    void testMissingTopLevelOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "openai",
                        "openai.api_key", "key");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining("One or more required options are missing.");
    }

    @Test
    void testMissingProviderLevelOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "bedrock",
                        "task", "text_generation",
                        "bedrock.endpoint", "endpoint",
                        "bedrock.aws_access_key_id", "key_id");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining("BEDROCK.AWS_SECRET_ACCESS_KEY");
    }

    @Test
    void testExtraTopLevelOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "openai",
                        "task", "text_generation",
                        "openai.api_key", "key",
                        "openai.endpoint", "endpoint",
                        "openai.system_prompt", "count prime numbers",
                        "proxy", "proxy");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining("Unsupported options:" + "\n" + "\n" + "PROXY");
    }

    @Test
    void testExtraProviderLevelOption() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "vertexai",
                        "task", "text_generation",
                        "vertexai.service_key", "key",
                        "vertexai.endpoint", "endpoint",
                        "vertexai.proxy", "endpoint");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining("Unsupported options:" + "\n" + "\n" + "VERTEXAI.PROXY");
    }

    @Test
    void testAllowParamsLevelOption() {
        Map<String, String> options =
                Map.of(
                        "provider", "vertexai",
                        "task", "text_generation",
                        "vertexai.service_key", "key",
                        "vertexai.endpoint", "endpoint",
                        "vertexai.params.temp", "0.7");
        validateCreateModelOptions("m1", options);
    }

    @Test
    void testNotAllowConfluentOption() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "openai",
                        "task", "text_generation",
                        "openai.api_key", "key",
                        "openai.endpoint", "endpoint",
                        "confluent.model.secret.encrypt_strategy", "kms");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining(
                        "Unsupported options:"
                                + "\n"
                                + "\n"
                                + "CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY");
    }

    @Test
    void testNotAllowedParamsLevelOption() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "openai",
                        "task", "text_generation",
                        "openai.api_key", "key",
                        "openai.endpoint", "endpoint",
                        "openai.parameters.temp", "0.7");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining(
                        "Unsupported options:" + "\n" + "\n" + "OPENAI.PARAMETERS.TEMP");
    }

    @Test
    void testAzureOpenAIOption() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "azureopenai",
                        "task", "text_generation",
                        "azureopenai.api_key", "key");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .hasMessageContaining(
                        "Missing required options are:" + "\n" + "\n" + "AZUREOPENAI.ENDPOINT");
    }

    @Test
    void testSageMakerOption() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "sagemaker",
                        "task", "text_generation",
                        "sagemaker.aws_access_key_id", "key_id",
                        "sagemaker.aws_secret_access_key", "secret_key",
                        "sagemaker.endpoint", "endpoint",
                        "sagemaker.custom_attributes", "c000b4f9-df62-4c85-a0bf-7c525f9104a4",
                        "sagemaker.inference_id", "inference_id",
                        "sagemaker.target_variant", "variant1",
                        "sagemaker.target_model", "model.tar.gz",
                        "sagemaker.target_container_host_name", "secondContainer");
        validateCreateModelOptions("m1", options);
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
    void testAzureMLOptionalProviderOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "azureml",
                        "task", "text_generation",
                        "azureml.api_key", "key_id",
                        "azureml.endpoint", "endpoint",
                        "azureml.output_format", "output_format",
                        "azureml.output_content_type", "output_content_type",
                        "azureml.deployment_name", "deployment_name");
        Map<String, String> publicOption = RemoteModelValidator.getPublicOptions(options);
        assertThat(publicOption).contains(entry("azureml.deployment_name", "deployment_name"));
    }

    @Test
    void testSageMakerOptionalProviderOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "sagemaker",
                        "task", "classification",
                        "sagemaker.aws_access_key_id", "key_id",
                        "sagemaker.aws_secret_access_key", "secret_key",
                        "sagemaker.endpoint", "endpoint",
                        "sagemaker.custom_attributes", "custom_attributes",
                        "sagemaker.inference_id", "inference_id",
                        "sagemaker.target_variant", "target_variant",
                        "sagemaker.target_model", "target_model");
        Map<String, String> publicOptions = RemoteModelValidator.getPublicOptions(options);
        assertThat(publicOptions)
                .contains(
                        entry("sagemaker.custom_attributes", "custom_attributes"),
                        entry("sagemaker.inference_id", "inference_id"),
                        entry("sagemaker.target_variant", "target_variant"),
                        entry("sagemaker.target_model", "target_model"));
    }

    @Test
    void testVertexAIOptionalProviderOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "vertexai",
                        "task", "classification",
                        "vertexai.endpoint", "endpoint",
                        "vertexai.api_key", "key");
        assertThatThrownBy(() -> RemoteModelValidator.validateCreateModelOptions("m1", options))
                .hasMessageContaining(
                        "Missing required options are:" + "\n" + "\n" + "VERTEXAI.SERVICE_KEY");
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
}
