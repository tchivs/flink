/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import io.confluent.flink.table.modules.ml.RemoteModelValidator.RemoteModelOptionsFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.confluent.flink.table.modules.ml.RemoteModelValidator.validateCreateModelOptions;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RemoteModelValidator}. */
public class RemoteModelValidatorTest {

    // <Provider, <valid, invalid>>
    public static Collection<Tuple2<String, Tuple2<List<String>, List<String>>>> endpoints() {
        List<Tuple2<String, Tuple2<List<String>, List<String>>>> ret =
                ImmutableList.of(
                        Tuple2.of(
                                MLModelSupportedProviders.AZUREML.name(),
                                Tuple2.of(
                                        ImmutableList.of(
                                                "https://ENDPOINT.REGION.inference.ml.azure.com/score",
                                                "https://ENDPOINT.REGION.inference.ai.azure.com/v1/completions"),
                                        ImmutableList.of(
                                                "http://ENDPOINT.REGION.inference.ml.azure.com/score",
                                                "https://region.inference.ml.azure.com/score"))),
                        Tuple2.of(
                                MLModelSupportedProviders.AZUREOPENAI.name(),
                                Tuple2.of(
                                        ImmutableList.of(
                                                "https://YOUR_RESOURCE_NAME.openai.azure.com/openai/deployments/YOUR_DEPLOYMENT_NAME/chat/completions?api-version=DATE"),
                                        ImmutableList.of(
                                                "http://YOUR_RESOURCE_NAME.openai.azure.com/openai/deployments/YOUR_DEPLOYMENT_NAME/chat/completions?api-version=DATE",
                                                "openai.azure.com/openai/deployments/YOUR_DEPLOYMENT_NAME/chat/completions?api-version=DATE"))),
                        Tuple2.of(
                                MLModelSupportedProviders.BEDROCK.name(),
                                Tuple2.of(
                                        ImmutableList.of(
                                                "https://bedrock-runtime.REGION.amazonaws.com/model/MODEL-NAME/invoke"),
                                        ImmutableList.of(
                                                "http://bedrock-runtime.REGION.amazonaws.com/model/MODEL-NAME/invoke",
                                                "https://REGION.amazonaws.com/model/MODEL-NAME/invoke"))),
                        Tuple2.of(
                                MLModelSupportedProviders.GOOGLEAI.name(),
                                Tuple2.of(
                                        ImmutableList.of(
                                                "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"),
                                        ImmutableList.of(
                                                "http://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent",
                                                "https://googleapis.com/v1beta/models/gemini-pro:generateContent"))),
                        Tuple2.of(
                                MLModelSupportedProviders.OPENAI.name(),
                                Tuple2.of(
                                        ImmutableList.of(
                                                "https://api.openai.com/v1/chat/completions"),
                                        ImmutableList.of(
                                                "http://api.openai.com/v1/chat/completions",
                                                "https://something.api.openai.com/v1/chat/completions"))),
                        Tuple2.of(
                                MLModelSupportedProviders.SAGEMAKER.name(),
                                Tuple2.of(
                                        ImmutableList.of(
                                                "https://runtime.sagemaker.REGION.amazonaws.com/endpoints/ENDPOINT/invocations"),
                                        ImmutableList.of(
                                                "http://runtime.sagemaker.REGION.amazonaws.com/endpoints/ENDPOINT/invocations",
                                                "https://runtime.sagemaker.REGION.amazonaws.com.other.com/endpoints/ENDPOINT/invocations"))),
                        Tuple2.of(
                                MLModelSupportedProviders.VERTEXAI.name(),
                                Tuple2.of(
                                        ImmutableList.of(
                                                "https://REGION-aiplatform.googleapis.com/v1/projects/PROJECT/locations/LOCATION/endpoints/ENDPOINT:predict"),
                                        ImmutableList.of(
                                                "http://REGION-aiplatform.googleapis.com/v1/projects/PROJECT/locations/LOCATION/endpoints/ENDPOINT:predict",
                                                "https://aiplatform.googleapis.com/v1/projects/PROJECT/locations/LOCATION/endpoints/ENDPOINT:predict"))));

        if (ret.size() != MLModelSupportedProviders.values().length) {
            throw new IllegalArgumentException("Missing providers");
        }

        return ret;
    }

    @Test
    void testUppercaseOptionsThrow() {
        Map<String, String> map = ImmutableMap.of("key", "value", "Key", "value");
        assertThatThrownBy(() -> RemoteModelValidator.uppercaseOptions(map))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Options contains both uppercase and lowercase of key: 'Key'");
    }

    @Test
    void testLowercaseOptionsThrow() {
        Map<String, String> map = ImmutableMap.of("key", "value", "Key", "value");
        assertThatThrownBy(() -> RemoteModelValidator.lowercaseOptions(map))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Options contains both uppercase and lowercase of key: 'Key'");
    }

    @Test
    void testUppercaseOptions() {
        Map<String, String> map = ImmutableMap.of("key1", "value1", "Key2", "value2");
        assertThat(RemoteModelValidator.uppercaseOptions(map))
                .isEqualTo(ImmutableMap.of("KEY1", "value1", "KEY2", "value2"));
    }

    @Test
    void testLowercaseOptions() {
        Map<String, String> map = ImmutableMap.of("KEY1", "Value1", "KeY2", "value2");
        assertThat(RemoteModelValidator.lowercaseOptions(map))
                .isEqualTo(ImmutableMap.of("key1", "Value1", "key2", "value2"));
    }

    @Test
    void testInvalidProviderOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "unknown",
                        "task", "text_generation",
                        "unknown.api_key", "key");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Unsupported PROVIDER: 'unknown'. Supported providers are [OPENAI, AZUREML, AZUREOPENAI, BEDROCK, SAGEMAKER, GOOGLEAI, VERTEXAI]");
    }

    @Test
    void testMissingProviderOptions() {
        Map<String, String> options = ImmutableMap.of("task", "text_generation");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'PROVIDER' is not set");
    }

    @Test
    void testMissingTopLevelOptions() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "openai",
                        "openai.api_key", "key");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .isInstanceOf(ValidationException.class)
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
                .isInstanceOf(ValidationException.class)
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
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options:" + "\n" + "\n" + "PROXY");
    }

    @Test
    void testInvalidEndpoint() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "openai",
                        "task", "text_generation",
                        "openai.api_key", "key",
                        "openai.endpoint", "http://api.openai.com/",
                        "openai.system_prompt", "count prime numbers");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "For OPENAI endpoint, the protocol should be https, got http://api.openai.com/");
    }

    @Test
    void testValidEndpoint() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "openai",
                        "task", "text_generation",
                        "openai.api_key", "key",
                        "openai.endpoint", "https://api.openai.com/",
                        "openai.system_prompt", "count prime numbers",
                        "openai.model_version", "chatgpt-4o");
        validateCreateModelOptions("m1", options);
    }

    @ParameterizedTest
    @MethodSource("endpoints")
    void testEndpointValidationAll(
            Tuple2<String, Tuple2<List<String>, List<String>>> providerTest) {
        String provider = providerTest.f0;
        RemoteModelOptions modelOptions =
                RemoteModelOptionsFactory.createRemoteModelOptions(provider);
        for (String validEndpoint : providerTest.f1.f0) {
            modelOptions.getProvider().validateEndpoint(validEndpoint, false);
        }

        for (String inValidEndpoint : providerTest.f1.f1) {
            assertThatThrownBy(
                            () ->
                                    modelOptions
                                            .getProvider()
                                            .validateEndpoint(inValidEndpoint, false))
                    .isInstanceOf(ValidationException.class);
        }
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
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options:" + "\n" + "\n" + "VERTEXAI.PROXY");
    }

    @Test
    void testAllowParamsLevelOption() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider", "vertexai",
                        "task", "text_generation",
                        "vertexai.service_key", "key",
                        "vertexai.endpoint",
                                "https://uscentral-1-aiplatform.googleapis.com/v1/projects/matrix/locations/uscentral-1/endpoints/ENDPOINT:predict",
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
                .isInstanceOf(ValidationException.class)
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
                .isInstanceOf(ValidationException.class)
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
                .isInstanceOf(ValidationException.class)
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
                        "sagemaker.endpoint",
                                "https://runtime.sagemaker.us-west-2.amazonaws.com/endpoints/matrix/invocations",
                        "sagemaker.custom_attributes", "c000b4f9-df62-4c85-a0bf-7c525f9104a4",
                        "sagemaker.inference_id", "inference_id",
                        "sagemaker.target_variant", "variant1",
                        "sagemaker.target_model", "model.tar.gz",
                        "sagemaker.model_version", "chatgpt-4o");
        validateCreateModelOptions("m1", options);
    }

    @Test
    void testParamLimit() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider",
                        "sagemaker",
                        "task",
                        "text_generation",
                        "sagemaker.aws_access_key_id",
                        "key_id",
                        "sagemaker.aws_secret_access_key",
                        "secret_key",
                        "sagemaker.endpoint",
                        "https://runtime.sagemaker.us-west-2.amazonaws.com/endpoints/matrix/invocations",
                        "sagemaker.params.p1",
                        "param1",
                        "sagemaker.params.p2",
                        "param2",
                        "sagemaker.params.p3",
                        "param3",
                        "sagemaker.target_container_host_name",
                        "secondContainer");
        validateCreateModelOptions("m1", options, 3);
    }

    @Test
    void testParamLimitViolation() {
        Map<String, String> options =
                ImmutableMap.of(
                        "provider",
                        "sagemaker",
                        "task",
                        "text_generation",
                        "sagemaker.aws_access_key_id",
                        "key_id",
                        "sagemaker.aws_secret_access_key",
                        "secret_key",
                        "sagemaker.endpoint",
                        "https://runtime.sagemaker.us-west-2.amazonaws.com/endpoints/matrix/invocations",
                        "sagemaker.params.p1",
                        "param1",
                        "SagemAker.Params.P2",
                        "param2",
                        "sagemaker.params.p3",
                        "param3",
                        "sagemaker.target_container_host_name",
                        "secondContainer");
        assertThatThrownBy(() -> validateCreateModelOptions("m1", options, 2))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Number of param options starting with 'sagemaker.params.' exceeds "
                                + "limit: 2. Current size: 3");
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
                        "sagemaker.target_model", "target_model",
                        "sagemaker.model_version", "chatgpt-4o");
        Map<String, String> publicOptions = RemoteModelValidator.getPublicOptions(options);
        assertThat(publicOptions)
                .contains(
                        entry("sagemaker.custom_attributes", "custom_attributes"),
                        entry("sagemaker.inference_id", "inference_id"),
                        entry("sagemaker.target_variant", "target_variant"),
                        entry("sagemaker.target_model", "target_model"),
                        entry("sagemaker.model_version", "chatgpt-4o"));
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
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Missing required options are:" + "\n" + "\n" + "VERTEXAI.SERVICE_KEY");
    }

    @Test
    void testCaseInsensitiveOptions() {
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
