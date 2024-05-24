/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import io.confluent.flink.table.modules.ml.options.AzureMLRemoteModelOptions;
import io.confluent.flink.table.modules.ml.options.AzureOpenAIRemoteModelOptions;
import io.confluent.flink.table.modules.ml.options.BedrockRemoteModelOptions;
import io.confluent.flink.table.modules.ml.options.GoogleAIRemoteModelOptions;
import io.confluent.flink.table.modules.ml.options.OpenAIRemoteModelOptions;
import io.confluent.flink.table.modules.ml.options.SageMakerRemoteModelOptions;
import io.confluent.flink.table.modules.ml.options.VertexAIRemoteModelOptions;
import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.CONFLUENT_PRIVATE_PREFIX;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.PROVIDER;
import static org.apache.flink.table.factories.FactoryUtil.validateFactoryOptions;
import static org.apache.flink.table.factories.FactoryUtil.validateRemainOptionKeys;
import static org.apache.flink.table.factories.FactoryUtil.validateUnconsumedKeys;

/**
 * Validation and enrichment utility for remote model public options coming directly from the user.
 */
public class RemoteModelValidator {

    private static final List<String> PRIVATE_PREFIXES = ImmutableList.of(CONFLUENT_PRIVATE_PREFIX);

    /**
     * Upper case options keys. Validation exception if options contains both uppercase and
     * lowercase for same key.
     *
     * @param options Options to uppercase
     * @return Map with uppercase key
     */
    public static Map<String, String> uppercaseOptions(Map<String, String> options) {
        return changeOptionsCase(options, true);
    }

    /**
     * Lower case options keys. Validation exception if options contains both uppercase and
     * lowercase for same key.
     *
     * @param options Options to uppercase
     * @return Map with lowercase key
     */
    public static Map<String, String> lowercaseOptions(Map<String, String> options) {
        return changeOptionsCase(options, false);
    }

    public static Map<String, String> changeOptionsCase(
            Map<String, String> options, boolean uppercase) {
        Map<String, String> newOptions = new HashMap<>();
        for (Entry<String, String> option : options.entrySet()) {
            String key =
                    uppercase
                            ? option.getKey().toUpperCase(Locale.ROOT)
                            : option.getKey().toLowerCase(Locale.ROOT);
            if (newOptions.containsKey(key)) {
                throw new ValidationException(
                        "Options contains both uppercase and lowercase of key: '"
                                + option.getKey()
                                + "'");
            }
            newOptions.put(key, option.getValue());
        }
        return newOptions;
    }

    public static Map<String, String> validateCreateModelOptions(
            String modelIdentifier, Map<String, String> options) {
        return validateCreateModelOptions(
                modelIdentifier, options, MLModelCommonConstants.DEFAULT_PARAM_SIZE_LIMIT);
    }

    public static Map<String, String> validateCreateModelOptions(
            String modelIdentifier, Map<String, String> options, int paramSizeLimit) {
        Map<String, String> uppercaseOptions = uppercaseOptions(options);
        final String provider = uppercaseOptions.get(PROVIDER);
        if (provider == null) {
            throw new ValidationException("'" + PROVIDER + "' is not set");
        }
        final PublicRemoteModelFactory factory =
                new PublicRemoteModelFactory(modelIdentifier, provider);
        final RemoteModelHelper helper =
                new RemoteModelHelper(factory, uppercaseOptions, paramSizeLimit);
        helper.validate();
        final Configuration validatedOptions = helper.getOptions();
        return validatedOptions.toMap();
    }

    /** Filter out public options from the given options. */
    public static Map<String, String> getPublicOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(
                        e ->
                                PRIVATE_PREFIXES.stream()
                                        .noneMatch(p -> e.getKey().toUpperCase().startsWith(p)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** Filter out private options from the given options. */
    public static Map<String, String> getPrivateOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(
                        e ->
                                PRIVATE_PREFIXES.stream()
                                        .anyMatch(p -> e.getKey().toUpperCase().startsWith(p)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** Filter out secret options from the given options. */
    public static Map<String, String> getSecretOptions(Map<String, String> options) {
        Map<String, String> caseInsensitiveOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveOptions.putAll(options);
        String provider = caseInsensitiveOptions.get(PROVIDER);
        RemoteModelOptions modelOptions =
                RemoteModelOptionsFactory.createRemoteModelOptions(provider);
        Set<String> secretKeys =
                modelOptions.getSecretOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toSet());
        return caseInsensitiveOptions.entrySet().stream()
                .filter(e -> secretKeys.contains(e.getKey().toUpperCase()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** Get all secret options from all providers. */
    public static Set<String> getAllSecretOptionsKeys() {
        Set<String> keys = new HashSet<>();
        for (MLModelSupportedProviders providers : MLModelSupportedProviders.values()) {
            RemoteModelOptions options =
                    RemoteModelOptionsFactory.createRemoteModelOptions(providers.getProviderName());
            keys.addAll(
                    options.getSecretOptions().stream()
                            .map(ConfigOption::key)
                            .collect(Collectors.toSet()));
        }
        return keys;
    }

    /** Factory to create RemoteModelOptions. */
    public static class RemoteModelOptionsFactory {
        public static RemoteModelOptions createRemoteModelOptions(String provider) {
            MLModelSupportedProviders modelProvider = getModelProvider(provider);
            switch (modelProvider) {
                case AZUREML:
                    return new AzureMLRemoteModelOptions();
                case AZUREOPENAI:
                    return new AzureOpenAIRemoteModelOptions();
                case OPENAI:
                    return new OpenAIRemoteModelOptions();
                case BEDROCK:
                    return new BedrockRemoteModelOptions();
                case GOOGLEAI:
                    return new GoogleAIRemoteModelOptions();
                case VERTEXAI:
                    return new VertexAIRemoteModelOptions();
                case SAGEMAKER:
                    return new SageMakerRemoteModelOptions();
                default:
                    throw new ValidationException("Unsupported '" + PROVIDER + "': " + provider);
            }
        }
    }

    private static class PublicRemoteModelFactory implements DynamicTableFactory {
        private final String modelIdentifier;
        private final RemoteModelOptions modelOptions;

        private PublicRemoteModelFactory(String modelIdentifier, String provider) {
            this.modelIdentifier = modelIdentifier;
            this.modelOptions = RemoteModelOptionsFactory.createRemoteModelOptions(provider);
        }

        @Override
        public String factoryIdentifier() {
            return modelIdentifier;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            final Set<ConfigOption<?>> options = new HashSet<>();
            options.addAll(modelOptions.getRequiredToplevelOptions());
            options.addAll(modelOptions.getRequiredProviderLevelOptions());
            return options;
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            final Set<ConfigOption<?>> options = new HashSet<>();
            options.addAll(modelOptions.getPublicOptionalTopLevelOptions());
            options.addAll(modelOptions.getOptionalProviderLevelOptions());
            return options;
        }

        public String paramsPrefix() {
            return modelOptions.getParamsPrefix();
        }

        public RemoteModelOptions getModelOptions() {
            return modelOptions;
        }
    }

    /** get MLModelSupportedProviders enum by provider string. */
    private static MLModelSupportedProviders getModelProvider(String provider) {
        for (MLModelSupportedProviders supportedProvider : MLModelSupportedProviders.values()) {
            if (supportedProvider.getProviderName().equalsIgnoreCase(provider)) {
                return supportedProvider;
            }
        }
        throw new ValidationException(
                "Unsupported "
                        + PROVIDER
                        + ": '"
                        + provider
                        + "'. Supported providers are "
                        + Arrays.toString(MLModelSupportedProviders.values()));
    }

    private static class RemoteModelHelper
            extends FactoryUtil.FactoryHelper<PublicRemoteModelFactory> {
        private final int paramSizeLimit;

        public RemoteModelHelper(
                PublicRemoteModelFactory factory, Map<String, String> options, int paramSizeLimit) {
            super(factory, options);
            this.paramSizeLimit = paramSizeLimit;
        }

        @Override
        public Configuration getOptions() {
            return allOptions;
        }

        /** validate model options with allowed prefix and throw exception if invalid. */
        @Override
        public void validate() {
            validateFactoryOptions(factory, allOptions);
            validateUnconsumedKeysWithAllowedPrefix(
                    factory.factoryIdentifier(),
                    allOptions.keySet(),
                    consumedOptionKeys,
                    deprecatedOptionKeys,
                    factory.paramsPrefix());
            validateEndpoint();
            validateParamSizeLimit();
        }

        /** Validate unconsumed options keys with skipping allowed prefix. */
        private static void validateUnconsumedKeysWithAllowedPrefix(
                String factoryIdentifier,
                Set<String> allOptionKeys,
                Set<String> consumedOptionKeys,
                Set<String> deprecatedOptionKeys,
                String allowedPrefix) {
            final Set<String> remainingOptionKeys = new HashSet<>(allOptionKeys);
            remainingOptionKeys.removeAll(consumedOptionKeys);
            // ignore allowed prefix if it is empty
            if (allowedPrefix.isEmpty()) {
                validateUnconsumedKeys(
                        factoryIdentifier, remainingOptionKeys, deprecatedOptionKeys);
            }
            remainingOptionKeys.removeIf(
                    key -> key.toUpperCase().startsWith(allowedPrefix.toUpperCase()));
            validateRemainOptionKeys(
                    factoryIdentifier,
                    consumedOptionKeys,
                    deprecatedOptionKeys,
                    remainingOptionKeys);
        }

        private void validateEndpoint() {
            factory.getModelOptions().validateEndpoint(allOptions, false);
        }

        private void validateParamSizeLimit() {
            String paramPrefix = factory.paramsPrefix().toLowerCase(Locale.ROOT);
            long paramCount =
                    allOptions.keySet().stream()
                            .filter(key -> key.toLowerCase(Locale.ROOT).startsWith(paramPrefix))
                            .count();
            if (paramCount > paramSizeLimit) {
                throw new ValidationException(
                        "Number of param options starting with '"
                                + paramPrefix
                                + "' exceeds limit: "
                                + paramSizeLimit
                                + ". Current size: "
                                + paramCount);
            }
        }
    }
}
