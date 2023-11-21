/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;

import io.confluent.flink.table.service.summary.QuerySummary;

/** A background job defined by a {@link CompiledPlan}. */
@Confluent
public class BackgroundJobResultPlan extends ResultPlan {

    private final String compiledPlan;

    public BackgroundJobResultPlan(QuerySummary querySummary, String compiledPlan) {
        super(querySummary);
        this.compiledPlan = compiledPlan;
    }

    public String getCompiledPlan() {
        return compiledPlan;
    }
}
