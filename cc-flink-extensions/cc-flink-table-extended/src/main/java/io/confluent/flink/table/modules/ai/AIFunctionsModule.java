/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Module to provide OpenAI built-in functions. */
public class AIFunctionsModule implements Module {

    public static final AIFunctionsModule INSTANCE = new AIFunctionsModule();
    Map<String, FunctionDefinition> normalizedFunctions;

    public AIFunctionsModule() {
        normalizedFunctions =
                new HashMap() {
                    {
                        put(AISecret.NAME, new AISecret());
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
