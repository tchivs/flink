/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;

import io.confluent.flink.table.service.summary.QuerySummary;

/** Common base class for foreground and background queries. */
@Confluent
public abstract class ResultPlan {

    private final QuerySummary querySummary;

    ResultPlan(QuerySummary querySummary) {
        this.querySummary = querySummary;
    }

    public QuerySummary getQuerySummary() {
        return querySummary;
    }
}
