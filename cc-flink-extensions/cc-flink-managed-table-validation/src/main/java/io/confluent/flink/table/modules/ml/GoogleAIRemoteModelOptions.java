/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.API_KEY;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.ENDPOINT;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.PARAMS_PREFIX;

/** Options for GoogleAI remote model. */
public class GoogleAIRemoteModelOptions extends RemoteModelOptions {
    private static final String NAMESPACE = MLModelSupportedProviders.GOOGLEAI.getProviderName();

    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.API_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The API key for the googleAI model.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.ENDPOINT)
                    .stringType()
                    .defaultValue(MLModelSupportedProviders.GOOGLEAI.getDefaultEndpoint())
                    .withDescription("The endpoint of the remote ML model.");

    public static final ConfigOption<String> SYSTEM_PROMPT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.SYSTEM_PROMPT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The system prompt for the googleAI model.");

    private final Set<ConfigOption<?>> requiredProviderLevelOptions = Set.of(API_KEY, ENDPOINT);
    private final Set<ConfigOption<?>> optionalProviderLevelOptions = Set.of(SYSTEM_PROMPT);
    private final Set<ConfigOption<?>> secrets = Set.of(API_KEY);
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
