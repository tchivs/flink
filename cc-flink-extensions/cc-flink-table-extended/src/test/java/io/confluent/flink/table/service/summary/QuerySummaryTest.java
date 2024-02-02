/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;

import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import io.confluent.flink.table.service.ResultPlanUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link QuerySummary}. */
@Confluent
public class QuerySummaryTest {

    @Test
    void testSummary() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final QuerySummary querySummary =
                assertProperties(
                        env,
                        "SELECT uid, LOWER(name) "
                                + "FROM (VALUES (1, 'Bob'), (2, 'Alice'), (3, 'John')) AS T(uid, name)",
                        QueryProperty.FOREGROUND,
                        QueryProperty.SINGLE_SINK);

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

    @Test
    void testBoundedness() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        createTable(env, "bounded1", Boundedness.BOUNDED);
        createTable(env, "bounded2", Boundedness.BOUNDED);
        createTable(env, "unbounded", Boundedness.CONTINUOUS_UNBOUNDED);

        assertProperties(env, "TABLE bounded1", QueryProperty.BOUNDED);
        assertProperties(env, "TABLE bounded1 UNION ALL TABLE bounded2", QueryProperty.BOUNDED);
        assertProperties(
                env,
                "TABLE bounded1 UNION ALL TABLE unbounded UNION ALL TABLE bounded2",
                QueryProperty.UNBOUNDED);
    }

    @Test
    void testChangelogMode() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        createTable(env, "unbounded", Boundedness.CONTINUOUS_UNBOUNDED);

        assertProperties(env, "SELECT * FROM unbounded", QueryProperty.APPEND_ONLY);
        assertProperties(env, "SELECT COUNT(*) FROM unbounded", QueryProperty.UPDATING);
    }

    @Test
    void testUpsertKeys() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        createTable(env, "unbounded1", Boundedness.CONTINUOUS_UNBOUNDED);
        createTable(env, "unbounded2", Boundedness.CONTINUOUS_UNBOUNDED);

        assertThat(assertProperties(env, "SELECT k, v FROM unbounded1", QueryProperty.APPEND_ONLY))
                .extracting(QuerySummary::getForegroundSinkUpsertKeys)
                .isEqualTo(new int[] {0});

        assertThat(assertProperties(env, "SELECT v FROM unbounded1", QueryProperty.APPEND_ONLY))
                .extracting(QuerySummary::getForegroundSinkUpsertKeys)
                .isEqualTo(new int[] {});

        assertThat(
                        assertProperties(
                                env,
                                "SELECT u1.k, u1.v, u2.k, u2.v "
                                        + "FROM unbounded1 u1 JOIN unbounded2 u2 ON u1.k = u1.k",
                                QueryProperty.APPEND_ONLY))
                .extracting(QuerySummary::getForegroundSinkUpsertKeys)
                .isEqualTo(new int[] {0, 2});
    }

    private static void createTable(TableEnvironment env, String name, Boundedness boundedness)
            throws Exception {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "datagen");
        if (boundedness == Boundedness.BOUNDED) {
            options.put("number-of-rows", "10");
        }
        ResultPlanUtils.createConfluentCatalogTable(
                env,
                name,
                Schema.newBuilder()
                        .column("k", DataTypes.INT().notNull())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .build(),
                options,
                Collections.emptyMap());
    }

    private static QuerySummary assertProperties(
            TableEnvironment env, String sql, QueryProperty... properties) throws Exception {
        final ForegroundJobResultPlan plan = ResultPlanUtils.foregroundJob(env, sql);
        final QuerySummary querySummary = plan.getQuerySummary();
        assertThat(querySummary.getProperties()).contains(properties);
        return querySummary;
    }
}
