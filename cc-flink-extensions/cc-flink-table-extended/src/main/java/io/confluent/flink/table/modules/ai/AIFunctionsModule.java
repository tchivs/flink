/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import io.confluent.flink.compute.credentials.InMemoryCredentialDecrypterImpl;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Module to provide OpenAI built-in functions. */
public class AIFunctionsModule implements Module {
    Map<String, FunctionDefinition> normalizedFunctions;

    public AIFunctionsModule(Map<String, String> sqlSecretsConfig) {
        this.normalizedFunctions =
                new HashMap() {
                    {
                        put(
                                AISecret.NAME,
                                new AISecret(
                                        InMemoryCredentialDecrypterImpl.INSTANCE,
                                        sqlSecretsConfig));
                        put(AIResponseGenerator.NAME, new AIResponseGenerator());
                    }
                };
    }

    @Override
    public Set<String> listFunctions() {
        return normalizedFunctions.keySet();
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        final String normalizedName = name.toUpperCase(Locale.ROOT);
        return Optional.ofNullable(normalizedFunctions.get(normalizedName));
    }
}
