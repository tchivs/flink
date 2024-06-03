/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Module to provide Federated Search built-in functions. */
public class FederatedSearchFunctionsModule implements Module {
    Map<String, FunctionDefinition> normalizedFunctions;

    public FederatedSearchFunctionsModule() {
        this.normalizedFunctions =
                new HashMap<String, FunctionDefinition>() {
                    {
                        put(
                                VectorSearchFunction.NAME,
                                new VectorSearchFunction(
                                        VectorSearchFunction.NAME,
                                        Collections.emptyMap(),
                                        Collections.emptyMap()));
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
        return Optional.ofNullable(this.normalizedFunctions.get(normalizedName));
    }
}
