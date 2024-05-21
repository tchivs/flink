/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.data.RowData;

import io.confluent.flink.table.service.summary.QuerySummary;

import java.util.stream.Stream;

/** A plan for serving results of foreground queries. */
@Confluent
public abstract class ForegroundResultPlan extends ResultPlan {

    private ForegroundResultPlan(QuerySummary querySummary) {
        super(querySummary);
    }

    /** A foreground job defined by a {@link CompiledPlan} with a result serving operator. */
    public static final class ForegroundJobResultPlan extends ForegroundResultPlan {

        private final String compiledPlan;

        private final String operatorId;

        public ForegroundJobResultPlan(
                QuerySummary querySummary, String compiledPlan, String operatorId) {
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

    /**
     * A foreground job that has been executed locally and serves results via {@link #getData()}.
     */
    public static final class ForegroundLocalResultPlan extends ForegroundResultPlan {

        private final Stream<RowData> data;

        public ForegroundLocalResultPlan(QuerySummary querySummary, Stream<RowData> data) {
            super(querySummary);
            this.data = data;
        }

        public Stream<RowData> getData() {
            return data;
        }
    }
}
