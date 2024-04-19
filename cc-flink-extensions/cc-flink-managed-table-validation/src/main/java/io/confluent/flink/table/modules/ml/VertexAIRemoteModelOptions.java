/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.PARAMS_PREFIX;

/** Options for VertexAI remote model. */
public class VertexAIRemoteModelOptions extends RemoteModelOptions {
    private static final String NAMESPACE = MLModelSupportedProviders.VERTEXAI.getProviderName();

    public static final ConfigOption<String> SERVICE_KEY =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.SERVICE_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The service key for the VertexAI model.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.ENDPOINT)
                    .stringType()
                    .defaultValue(MLModelSupportedProviders.VERTEXAI.getDefaultEndpoint())
                    .withDescription("The endpoint of the remote ML model.");

    public static final ConfigOption<String> SYSTEM_PROMPT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.SYSTEM_PROMPT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The system prompt for the VertexAI model.");

    private final Set<ConfigOption<?>> requiredProviderLevelOptions =
            ImmutableSet.of(SERVICE_KEY, ENDPOINT);
    private final Set<ConfigOption<?>> optionalProviderLevelOptions =
            ImmutableSet.of(SYSTEM_PROMPT);
    private final Set<ConfigOption<?>> secrets = ImmutableSet.of(SERVICE_KEY);
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
    public String getparamsPrefix() {
        return paramsPrefix;
    }
}
