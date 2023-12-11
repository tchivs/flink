/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import java.util.Collections;

/** An interceptor that allows runtime changes to the URL hostname. */
public class AIFunctionsTestModule extends AIFunctionsModule {
    public AIFunctionsTestModule(String testBaseUrl) {
        super.normalizedFunctions =
                Collections.singletonMap(
                        AIResponseGenerator.NAME, new AIResponseGenerator(testBaseUrl));
    }
}
