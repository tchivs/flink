/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
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

    public static Map<String, String> validateCreateModelOptions(
            String modelIdentifier, Map<String, String> options) {
        Map<String, String> uppercaseOptions =
                options.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getKey().toUpperCase(), Entry::getValue));
        final String provider = uppercaseOptions.get(PROVIDER);
        if (provider == null) {
            throw new IllegalArgumentException("'" + PROVIDER + "' is not set");
        }
        final PublicRemoteModelFactory factory =
                new PublicRemoteModelFactory(modelIdentifier, provider);
        final RemoteModelHelper helper = new RemoteModelHelper(factory, uppercaseOptions);
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

    private static class RemoteModelOptionsFactory {
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
                    throw new IllegalArgumentException(
                            "Unsupported '" + PROVIDER + "': " + provider);
            }
        }
    }

    private static class PublicRemoteModelFactory implements DynamicTableFactory {
        private final String modelIdentifier;
        private final String provider;

        private PublicRemoteModelFactory(String modelIdentifier, String provider) {
            this.modelIdentifier = modelIdentifier;
            this.provider = provider;
        }

        @Override
        public String factoryIdentifier() {
            return modelIdentifier;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            RemoteModelOptions modelOptions =
                    RemoteModelOptionsFactory.createRemoteModelOptions(provider);
            final Set<ConfigOption<?>> options = new HashSet<>();
            options.addAll(modelOptions.getRequiredToplevelOptions());
            options.addAll(modelOptions.getRequiredProviderLevelOptions());
            return options;
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            RemoteModelOptions modelOptions =
                    RemoteModelOptionsFactory.createRemoteModelOptions(provider);
            final Set<ConfigOption<?>> options = new HashSet<>();
            options.addAll(modelOptions.getPublicOptionalTopLevelOptions());
            options.addAll(modelOptions.getOptionalProviderLevelOptions());
            return options;
        }

        public String paramsPrefix() {
            RemoteModelOptions modelOptions =
                    RemoteModelOptionsFactory.createRemoteModelOptions(provider);
            return modelOptions.getParamsPrefix();
        }
    }

    /** get MLModelSupportedProviders enum by provider string. */
    private static MLModelSupportedProviders getModelProvider(String provider) {
        for (MLModelSupportedProviders supportedProvider : MLModelSupportedProviders.values()) {
            if (supportedProvider.getProviderName().equalsIgnoreCase(provider)) {
                return supportedProvider;
            }
        }
        throw new IllegalArgumentException("Unsupported '" + PROVIDER + "': " + provider);
    }

    private static class RemoteModelHelper
            extends FactoryUtil.FactoryHelper<PublicRemoteModelFactory> {
        public RemoteModelHelper(PublicRemoteModelFactory factory, Map<String, String> options) {
            super(factory, options);
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
    }
}
