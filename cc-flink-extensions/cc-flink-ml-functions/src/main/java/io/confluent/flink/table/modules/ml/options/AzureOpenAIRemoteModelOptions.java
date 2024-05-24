/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.PARAMS_PREFIX;

/** Options for Azure OpenAI remote model. */
public class AzureOpenAIRemoteModelOptions extends OpenAIRemoteModelOptions {

    private static final String NAMESPACE = MLModelSupportedProviders.AZUREOPENAI.getProviderName();

    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.API_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The API key for the Azure OpenAI model.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.ENDPOINT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The endpoint of the remote ML model.");

    public static final ConfigOption<String> SYSTEM_PROMPT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.SYSTEM_PROMPT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The system prompt for the text generation model.");

    public static final ConfigOption<String> INPUT_FORMAT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.INPUT_FORMAT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The input format for the remote ML model.");

    public static final ConfigOption<String> INPUT_CONTENT_TYPE =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.INPUT_CONTENT_TYPE)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The input content type for the remote ML model.");

    public static final ConfigOption<String> OUTPUT_FORMAT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.OUTPUT_FORMAT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The output format for the remote ML model.");

    public static final ConfigOption<String> OUTPUT_CONTENT_TYPE =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.OUTPUT_CONTENT_TYPE)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The output content type for the remote ML model.");

    public static final ConfigOption<String> MODEL_VERSION =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.PROVIDER_MODEL_VERSION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The version of the remote ML model.");

    private final Set<ConfigOption<?>> requiredProviderLevelOptions =
            ImmutableSet.of(API_KEY, ENDPOINT);
    private final Set<ConfigOption<?>> optionalProviderLevelOptions =
            ImmutableSet.of(
                    SYSTEM_PROMPT,
                    MODEL_VERSION,
                    INPUT_FORMAT,
                    INPUT_CONTENT_TYPE,
                    OUTPUT_FORMAT,
                    OUTPUT_CONTENT_TYPE);
    private final Set<ConfigOption<?>> secrets = ImmutableSet.of(API_KEY);
    private final String paramsPrefix = NAMESPACE + "." + PARAMS_PREFIX;

    @Override
    public Set<ConfigOption<?>> getRequiredProviderLevelOptions() {
        return requiredProviderLevelOptions;
    }

    @Override
    public Set<ConfigOption<?>> getOptionalProviderLevelOptions() {
        return optionalProviderLevelOptions;
    }

    @Override
    public Set<ConfigOption<?>> getSecretOptions() {
        return secrets;
    }

    @Override
    public String getParamsPrefix() {
        return paramsPrefix;
    }

    @Override
    public void validateEndpoint(Configuration configuration, boolean runtime) {
        String endpoint = configuration.getString(ENDPOINT);
        getProvider().validateEndpoint(endpoint, runtime);
    }

    @Override
    public MLModelSupportedProviders getProvider() {
        return MLModelSupportedProviders.AZUREOPENAI;
    }
}
