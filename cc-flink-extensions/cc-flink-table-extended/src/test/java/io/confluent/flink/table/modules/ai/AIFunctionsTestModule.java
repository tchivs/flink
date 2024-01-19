/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.FunctionDefinition;

import io.confluent.flink.credentials.CredentialDecrypter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** An interceptor that allows runtime changes to the URL hostname. */
public class AIFunctionsTestModule extends AIFunctionsModule {
    public AIFunctionsTestModule(
            String testBaseUrl,
            CredentialDecrypter decrypter,
            Configuration aiFunctionsConfig,
            Map<String, String> sqlSecretsConfig) {
        super(aiFunctionsConfig, Collections.emptyMap());
        super.normalizedFunctions =
                new HashMap<String, FunctionDefinition>() {
                    {
                        put(AISecret.NAME, new AISecret(decrypter, sqlSecretsConfig));
                        put(
                                AIResponseGenerator.NAME,
                                new AIResponseGenerator(aiFunctionsConfig, testBaseUrl));
                    }
                };
    }
}
