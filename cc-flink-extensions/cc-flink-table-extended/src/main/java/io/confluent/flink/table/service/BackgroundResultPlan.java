/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;

/** {@link CompiledPlan} for a background job. */
@Confluent
public class BackgroundResultPlan {

    private final String compiledPlan;

    public BackgroundResultPlan(String compiledPlan) {
        this.compiledPlan = compiledPlan;
    }

    public String getCompiledPlan() {
        return compiledPlan;
    }
}
