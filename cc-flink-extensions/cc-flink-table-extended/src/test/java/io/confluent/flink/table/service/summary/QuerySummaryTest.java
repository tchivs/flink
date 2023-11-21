/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import io.confluent.flink.table.service.ResultPlanUtils;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link QuerySummary}. */
@Confluent
public class QuerySummaryTest {

    @Test
    void testSummary() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJob(
                        tableEnv,
                        "SELECT uid, LOWER(name) "
                                + "FROM (VALUES (1, 'Bob'), (2, 'Alice'), (3, 'John')) AS T(uid, name)");

        final QuerySummary querySummary = plan.getQuerySummary();

        assertThat(querySummary.getProperties())
                .containsExactlyInAnyOrder(QueryProperty.FOREGROUND, QueryProperty.SINGLE_SINK);

        assertThat(querySummary.getNodeKinds())
                .containsExactlyInAnyOrder(NodeKind.VALUES, NodeKind.CALC, NodeKind.SINK);

        assertThat(querySummary.getExpressionKinds())
                .containsExactlyInAnyOrder(ExpressionKind.INPUT_REF, ExpressionKind.OTHER);

        final NodeSummary sinkSummary = querySummary.getNodes().get(0);
        assertThat(sinkSummary).extracting(NodeSummary::getKind).isEqualTo(NodeKind.SINK);

        final NodeSummary calcSummary = sinkSummary.getInputs().get(0);
        assertThat(calcSummary).extracting(NodeSummary::getKind).isEqualTo(NodeKind.CALC);
        assertThat(calcSummary.getTag(NodeTag.EXPRESSIONS, Set.class)).isNotNull().hasSize(2);
    }
}
