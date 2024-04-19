/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.PARAMS_PREFIX;

/** Options for Azure OpenAI remote model. */
public class AzureMLRemoteModelOptions extends RemoteModelOptions {
    public static final String NAMESPACE = MLModelSupportedProviders.AZUREML.getProviderName();

    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.API_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The API key for the AzureML model.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.ENDPOINT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The endpoint of the remote ML model.");

    private final Set<ConfigOption<?>> requiredProviderLevelOptions =
            ImmutableSet.of(API_KEY, ENDPOINT);
    private final Set<ConfigOption<?>> optionalProviderLevelOptions = ImmutableSet.of();
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
    public String getparamsPrefix() {
        return paramsPrefix;
    }
}
