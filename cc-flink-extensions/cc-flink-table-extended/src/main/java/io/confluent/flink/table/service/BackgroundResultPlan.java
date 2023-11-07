/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;

import io.confluent.flink.table.service.summary.QuerySummary;

/** {@link CompiledPlan} for a background job. */
@Confluent
public class BackgroundResultPlan extends ResultPlan {

    private final String compiledPlan;

    public BackgroundResultPlan(QuerySummary querySummary, String compiledPlan) {
        super(querySummary);
        this.compiledPlan = compiledPlan;
    }

    public String getCompiledPlan() {
        return compiledPlan;
    }
}
