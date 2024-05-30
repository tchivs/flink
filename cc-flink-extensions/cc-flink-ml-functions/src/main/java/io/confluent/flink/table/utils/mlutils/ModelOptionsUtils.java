/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.mlutils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.table.modules.ml.MLModelCommonConstants;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.DEFAULT_VERSION;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.ENCRYPT_STRATEGY;

/** Utility class to fetch model options with provider namespace. */
public class ModelOptionsUtils {
    private final String provider;
    private final Map<String, String> caseInsensitiveModelOptions;

    public static String getProvider(Map<String, String> caseSensitiveModelOptions) {
        return caseSensitiveModelOptions.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(MLModelCommonConstants.PROVIDER))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse("");
    }

    public static MLModelCommonConstants.ModelKind getModelKind(Map<String, String> modelOptions) {
        final TreeMap<String, String> caseInSensitiveModelOptions =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInSensitiveModelOptions.putAll(modelOptions);
        if (caseInSensitiveModelOptions.containsKey(MLModelCommonConstants.PROVIDER)) {
            return MLModelCommonConstants.ModelKind.REMOTE;
        }
        throw new ValidationException("'provider' must be specified for model.");
    }

    public static MLModelCommonConstants.ModelTask getModelTask(Map<String, String> modelOptions) {
        final TreeMap<String, String> caseInSensitiveModelOptions =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInSensitiveModelOptions.putAll(modelOptions);
        if (!caseInSensitiveModelOptions.containsKey(MLModelCommonConstants.TASK)) {
            throw new ValidationException("'task' must be specified for model.");
        }
        final String taskStringValue =
                caseInSensitiveModelOptions.get(MLModelCommonConstants.TASK).toUpperCase();
        MLModelCommonConstants.ModelTask taskValue;
        try {
            taskValue = MLModelCommonConstants.ModelTask.valueOf(taskStringValue);
        } catch (IllegalArgumentException e) {
            taskValue = null;
        }
        if (taskValue == null) {
            throw new ValidationException(
                    "Task value '"
                            + taskStringValue
                            + "' is not supported. Supported values are: "
                            + Arrays.toString(MLModelCommonConstants.ModelTask.values()));
        }
        return taskValue;
    }

    public ModelOptionsUtils(CatalogModel model) {
        this(model.getOptions());
    }

    public ModelOptionsUtils(CatalogModel model, String provider) {
        this.provider = provider;
        caseInsensitiveModelOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveModelOptions.putAll(model.getOptions());
    }

    public ModelOptionsUtils(Map<String, String> modelOptions) {
        this.provider = getProvider(modelOptions);
        caseInsensitiveModelOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveModelOptions.putAll(modelOptions);
    }

    public String getProviderOption(String optionName) {
        return caseInsensitiveModelOptions.get((provider + "." + optionName).toUpperCase());
    }

    public String getProviderOptionOrDefault(String optionName, String defaultValue) {
        return caseInsensitiveModelOptions.getOrDefault(
                (provider + "." + optionName).toUpperCase(), defaultValue);
    }

    public Map<String, String> getCaseInsensitiveProviderOptionsStartingWith(String prefix) {
        Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String namespacePrefix = provider + "." + prefix;
        caseInsensitiveModelOptions.forEach(
                (key, value) -> {
                    if (key.toUpperCase().startsWith(namespacePrefix.toUpperCase())) {
                        // Remove the namespace prefix from the key.
                        options.put(key.substring(namespacePrefix.length()), value);
                    }
                });
        return options;
    }

    public String getEncryptStrategy() {
        return caseInsensitiveModelOptions.get(ENCRYPT_STRATEGY);
    }

    public String getDefaultVersion() {
        return caseInsensitiveModelOptions.get(DEFAULT_VERSION);
    }

    public String getCredentialServiceHost() {
        return caseInsensitiveModelOptions.get(MLModelCommonConstants.CREDENTIAL_SERVICE_HOST);
    }

    public int getCredentialServicePort() {
        try {
            String port =
                    caseInsensitiveModelOptions.get(MLModelCommonConstants.CREDENTIAL_SERVICE_PORT);
            return Integer.parseInt(port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    MLModelCommonConstants.CREDENTIAL_SERVICE_PORT + " should be a number", e);
        }
    }

    public String getOption(final String key) {
        return caseInsensitiveModelOptions.get(key);
    }

    public String getOptionOrDefault(final String key, final String defaultValue) {
        String value = caseInsensitiveModelOptions.get(key);
        return value == null ? defaultValue : value;
    }

    public String getProvider() {
        return provider;
    }
}
