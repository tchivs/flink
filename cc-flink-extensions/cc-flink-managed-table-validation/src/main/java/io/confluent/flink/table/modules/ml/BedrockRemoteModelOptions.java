/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_ACCESS_KEY_ID;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_SECRET_ACCESS_KEY;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_SESSION_TOKEN;

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

    private final Set<ConfigOption<?>> requiredProviderLevelOptions =
            ImmutableSet.of(ACCESS_KEY_ID, SECRET_KEY, ENDPOINT);
    private final Set<ConfigOption<?>> optionalProviderLevelOptions =
            ImmutableSet.of(
                    SESSION_TOKEN,
                    SYSTEM_PROMPT,
                    INPUT_FORMAT,
                    INPUT_CONTENT_TYPE,
                    OUTPUT_FORMAT,
                    OUTPUT_CONTENT_TYPE);
    private final Set<ConfigOption<?>> secrets =
            ImmutableSet.of(ACCESS_KEY_ID, SECRET_KEY, SESSION_TOKEN);
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
    public String getParamsPrefix() {
        return paramsPrefix;
    }
}
