/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;

import io.confluent.flink.table.service.summary.QuerySummary;

/** {@link CompiledPlan} for foreground result serving queries. */
@Confluent
public final class ForegroundResultPlan extends ResultPlan {

    private final String compiledPlan;

    private final String operatorId;

    public ForegroundResultPlan(QuerySummary querySummary, String compiledPlan, String operatorId) {
        super(querySummary);
        this.compiledPlan = compiledPlan;
        this.operatorId = operatorId;
    }

    public String getCompiledPlan() {
        return compiledPlan;
    }

    public String getOperatorId() {
        return operatorId;
    }
}
