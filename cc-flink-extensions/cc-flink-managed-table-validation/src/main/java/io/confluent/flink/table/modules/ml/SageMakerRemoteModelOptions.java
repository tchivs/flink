/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_ACCESS_KEY_ID;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_SECRET_ACCESS_KEY;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.AWS_SESSION_TOKEN;

/** Options for SageMaker remote model. */
public class SageMakerRemoteModelOptions extends RemoteModelOptions {
    private static final String NAMESPACE = MLModelSupportedProviders.SAGEMAKER.getProviderName();

    public static final ConfigOption<String> ACCESS_KEY_ID =
            ConfigOptions.key(NAMESPACE + "." + AWS_ACCESS_KEY_ID)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS access key id for the SageMaker model.");

    public static final ConfigOption<String> SECRET_KEY =
            ConfigOptions.key(NAMESPACE + "." + AWS_SECRET_ACCESS_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS secret key for the SageMaker model.");

    public static final ConfigOption<String> SESSION_TOKEN =
            ConfigOptions.key(NAMESPACE + "." + AWS_SESSION_TOKEN)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS session token for the SageMaker model.");

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

    public static final ConfigOption<String> CUSTOM_ATTRIBUTES =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.CUSTOM_ATTRIBUTES)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom attributes for the SageMaker model.");

    public static final ConfigOption<String> INFERENCE_ID =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.INFERENCE_ID)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inference id for the SageMaker model.");

    public static final ConfigOption<String> TARGET_VARIANT =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.TARGET_VARIANT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target variant for the SageMaker model.");

    public static final ConfigOption<String> TARGET_MODEL =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.TARGET_MODEL)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target model for the SageMaker model.");

    public static final ConfigOption<String> TARGET_CONTAINER_HOST_NAME =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.TARGET_CONTAINER_HOST_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target container host name for the SageMaker model.");

    public static final ConfigOption<String> INFERENCE_COMPONENT_NAME =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.INFERENCE_COMPONENT_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inference component name for the SageMaker model.");

    public static final ConfigOption<String> ENABLE_EXPLANATIONS =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.ENABLE_EXPLANATIONS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Enable explanations for the SageMaker model.");

    public static final ConfigOption<String> MODEL_VERSION =
            ConfigOptions.key(NAMESPACE + "." + MLModelCommonConstants.PROVIDER_MODEL_VERSION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The version of the remote ML model.");

    private final Set<ConfigOption<?>> requiredProviderLevelOptions =
            ImmutableSet.of(ACCESS_KEY_ID, SECRET_KEY, ENDPOINT);

    private final Set<ConfigOption<?>> optionalProviderLevelOptions =
            ImmutableSet.of(
                    SESSION_TOKEN,
                    SYSTEM_PROMPT,
                    MODEL_VERSION,
                    INPUT_FORMAT,
                    INPUT_CONTENT_TYPE,
                    OUTPUT_FORMAT,
                    OUTPUT_CONTENT_TYPE,
                    CUSTOM_ATTRIBUTES,
                    INFERENCE_ID,
                    TARGET_VARIANT,
                    TARGET_MODEL,
                    TARGET_CONTAINER_HOST_NAME,
                    INFERENCE_COMPONENT_NAME,
                    ENABLE_EXPLANATIONS);
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

    @Override
    public void validateEndpoint(Configuration configuration, boolean runtime) {
        String endpoint = configuration.getString(ENDPOINT);
        getProvider().validateEndpoint(endpoint, runtime);
    }

    @Override
    public MLModelSupportedProviders getProvider() {
        return MLModelSupportedProviders.SAGEMAKER;
    }
}
