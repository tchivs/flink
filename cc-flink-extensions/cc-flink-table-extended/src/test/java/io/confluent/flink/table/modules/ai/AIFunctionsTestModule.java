/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import io.confluent.flink.credentials.CredentialDecrypter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** An interceptor that allows runtime changes to the URL hostname. */
public class AIFunctionsTestModule extends AIFunctionsModule {
    public AIFunctionsTestModule(
            String testBaseUrl,
            CredentialDecrypter decrypter,
            Map<String, String> sqlSecretsConfig) {
        super(Collections.emptyMap());
        super.normalizedFunctions =
                new HashMap() {
                    {
                        put(AISecret.NAME, new AISecret(decrypter, sqlSecretsConfig));
                        put(AIResponseGenerator.NAME, new AIResponseGenerator(testBaseUrl));
                    }
                };
    }
}
