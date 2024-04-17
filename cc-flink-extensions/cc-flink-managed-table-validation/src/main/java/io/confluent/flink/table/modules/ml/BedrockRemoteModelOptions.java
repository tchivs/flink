/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_ACCESS_KEY_ID;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_SECRET_ACCESS_KEY;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_SESSION_TOKEN;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.ENDPOINT;

/** Options for Bedrock remote model. */
public class BedrockRemoteModelOptions extends RemoteModelOptions {
    private static final String NAMESPACE = MLModelSupportedProviders.BEDROCK.getProviderName();

    public static final ConfigOption<String> ACCESS_KEY_ID =
            ConfigOptions.key(NAMESPACE + "." + AWS_ACCESS_KEY_ID)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS access key id for the Bedrock model.");

    public static final ConfigOption<String> SECRET_KEY =
            ConfigOptions.key(NAMESPACE + "." + AWS_SECRET_ACCESS_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS secret key for the Bedrock model.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.ENDPOINT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The endpoint of the Bedrock model.");

    public static final ConfigOption<String> SESSION_TOKEN =
            ConfigOptions.key(NAMESPACE + "." + AWS_SESSION_TOKEN)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS session token for the Bedrock model.");

    public static final ConfigOption<String> SYSTEM_PROMPT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.SYSTEM_PROMPT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The system prompt for the Bedrock model.");

    private final Set<ConfigOption<?>> requiredProviderLevelOptions =
            Set.of(ACCESS_KEY_ID, SECRET_KEY, ENDPOINT);
    private final Set<ConfigOption<?>> optionalProviderLevelOptions =
            Set.of(SESSION_TOKEN, SYSTEM_PROMPT);
    private final Set<ConfigOption<?>> secrets = Set.of(ACCESS_KEY_ID, SECRET_KEY, SESSION_TOKEN);
    private final String paramsPrefix = NAMESPACE + "." + MLModelCommonConstants.PARAMS_PREFIX;

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
