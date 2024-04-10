/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogModel.ModelKind;
import org.apache.flink.table.catalog.CatalogModel.ModelTask;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Map;
import java.util.TreeMap;

/** Utility class to fetch model options with provider namespace. */
public class ModelOptionsUtils {
    public static final String PROVIDER_OPTION_KEY = "PROVIDER";
    private final String namespace;
    private final Map<String, String> caseInsensitiveModelOptions;
    // todo: matrix-74 Have a central place define all model constant
    private static final String ENCRYPT_STRATEGY = "confluent.model.secret.encrypt_strategy";

    public static String getProvider(Map<String, String> caseSensitiveModelOptions) {
        return caseSensitiveModelOptions.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(PROVIDER_OPTION_KEY))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse("");
    }

    public static ModelTask getModelTask(Map<String, String> caseInsensitiveModelOptions) {
        return FactoryUtil.getModelTask(caseInsensitiveModelOptions);
    }

    public static ModelKind getModelKind(Map<String, String> caseInsensitiveModelOptions) {
        return FactoryUtil.getModelKind(caseInsensitiveModelOptions);
    }

    public ModelOptionsUtils(CatalogModel model, String namespace) {
        this.namespace = namespace;
        caseInsensitiveModelOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveModelOptions.putAll(model.getOptions());
    }

    public ModelOptionsUtils(Map<String, String> modelOptions) {
        this.namespace = getProvider(modelOptions);
        caseInsensitiveModelOptions = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveModelOptions.putAll(modelOptions);
    }

    public String getProviderOption(String optionName) {
        return caseInsensitiveModelOptions.get((namespace + "." + optionName).toUpperCase());
    }

    public String getProviderOptionOrDefault(String optionName, String defaultValue) {
        return caseInsensitiveModelOptions.getOrDefault(
                (namespace + "." + optionName).toUpperCase(), defaultValue);
    }

    public Map<String, String> getCaseInsensitiveProviderOptionsStartingWith(String prefix) {
        Map<String, String> options = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        String namespacePrefix = namespace + "." + prefix;
        caseInsensitiveModelOptions.forEach(
                (key, value) -> {
                    if (key.toUpperCase().startsWith(namespacePrefix.toUpperCase())) {
                        // Remove the namespace prefix from the key.
                        options.put(key.substring(namespacePrefix.length()), value);
                    }
                });
        return options;
    }

    public Boolean isEncryptStrategyPlaintext() {
        String encryptStrategy = caseInsensitiveModelOptions.get(ENCRYPT_STRATEGY);
        if (encryptStrategy == null) {
            return false;
        }
        return encryptStrategy.equalsIgnoreCase("plaintext");
    }
}
