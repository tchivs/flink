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
import org.apache.flink.table.functions.ScalarFunction;

import io.confluent.flink.table.modules.remoteudf.ConfiguredFunctionSpec;
import io.confluent.flink.table.modules.remoteudf.ConfiguredRemoteScalarFunction;
import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import io.confluent.flink.table.service.ResultPlanUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link QuerySummary}. */
@Confluent
public class QuerySummaryTest {

    private static final List<ConfiguredFunctionSpec> UDF_SPECS =
            ConfiguredFunctionSpec.newBuilder()
                    .setOrganization("testOrg")
                    .setEnvironment("testEnv")
                    .setCatalog("a")
                    .setDatabase("b")
                    .setName("c")
                    .setPluginId("plugin")
                    .setPluginVersionId("v")
                    .setClassName("io.confluent.Foo")
                    .parseIsDeterministic("true")
                    .addArgumentTypes(Collections.singletonList("INT"))
                    .addArgumentTypes(Collections.singletonList("STRING"))
                    .addReturnType("INT")
                    .addReturnType("STRING")
                    .build();

    private static final List<ConfiguredFunctionSpec> UDF_SPECS2 =
            ConfiguredFunctionSpec.newBuilder()
                    .setOrganization("testOrg")
                    .setEnvironment("testEnv")
                    .setCatalog("a")
                    .setDatabase("b")
                    .setName("d")
                    .setPluginId("plugin")
                    .setPluginVersionId("v")
                    .setClassName("io.confluent.Foo")
                    .parseIsDeterministic("true")
                    .addArgumentTypes(Collections.singletonList("INT"))
                    .addReturnType("INT")
                    .build();

    @Test
    void testSummary() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporaryFunction("a.b.c", new ConfiguredRemoteScalarFunction(UDF_SPECS));

        final QuerySummary querySummary =
                assertProperties(
                        env,
                        "SELECT a.b.c(uid), LOWER(name) "
                                + "FROM (VALUES (1, 'Bob'), (2, 'Alice'), (3, 'John')) AS T(uid, name)",
                        QueryProperty.FOREGROUND,
                        QueryProperty.SINGLE_SINK);

        assertThat(querySummary.getNodeKinds())
                .containsExactlyInAnyOrder(NodeKind.VALUES, NodeKind.CALC, NodeKind.SINK);

        assertThat(querySummary.getExpressionKinds())
                .containsExactlyInAnyOrder(ExpressionKind.INPUT_REF, ExpressionKind.OTHER);

        assertThat(querySummary.getUdfCalls()).hasSize(1);
        assertThat(querySummary.getUdfCalls().iterator().next().getPath()).isEqualTo("`a`.`b`.`c`");
        assertThat(querySummary.getUdfCalls().iterator().next().getResourceConfigs().size())
                .isGreaterThan(0);

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

    @Test
    void testUdfUnused() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporaryFunction("a.b.c", new ConfiguredRemoteScalarFunction(UDF_SPECS));

        final QuerySummary querySummary =
                assertProperties(
                        env,
                        "SELECT uid, LOWER(name) "
                                + "FROM (VALUES (1, 'Bob'), (2, 'Alice'), (3, 'John')) AS T(uid, name)",
                        QueryProperty.FOREGROUND,
                        QueryProperty.SINGLE_SINK);

        assertThat(querySummary.getUdfCalls()).hasSize(0);
    }

    @Test
    void testNonUdfUsed() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporarySystemFunction("func", MyScalarFunction.class);

        final QuerySummary querySummary =
                assertProperties(
                        env,
                        "SELECT func(uid), LOWER(name) "
                                + "FROM (VALUES (1, 'Bob'), (2, 'Alice'), (3, 'John')) AS T(uid, name)",
                        QueryProperty.FOREGROUND,
                        QueryProperty.SINGLE_SINK);

        assertThat(querySummary.getUdfCalls()).hasSize(0);
    }

    @Test
    void testUdfNested() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporaryFunction("a.b.c", new ConfiguredRemoteScalarFunction(UDF_SPECS));
        env.createTemporaryFunction("a.b.d", new ConfiguredRemoteScalarFunction(UDF_SPECS2));

        final QuerySummary querySummary =
                assertProperties(
                        env,
                        "SELECT a.b.c(a.b.d(uid)), a.b.d(uid), LOWER(name) "
                                + "FROM (VALUES (1, 'Bob'), (2, 'Alice'), (3, 'John')) AS T(uid, name)",
                        QueryProperty.FOREGROUND,
                        QueryProperty.SINGLE_SINK);

        assertThat(querySummary.getUdfCalls()).hasSize(2);
        List<String> names =
                querySummary.getUdfCalls().stream()
                        .map(call -> call.getPath())
                        .collect(Collectors.toList());
        assertThat(names).containsExactlyInAnyOrder("`a`.`b`.`c`", "`a`.`b`.`d`");
    }

    @Test
    void testUdfRequiresNonUdf() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporaryFunction("func", new ConfiguredRemoteScalarFunction(UDF_SPECS));
        env.createTemporarySystemFunction("system_func", MyScalarFunction.class);

        final QuerySummary querySummary =
                assertProperties(
                        env,
                        "SELECT func(system_func(uid)), LOWER(name) "
                                + "FROM (VALUES (1, 'Bob'), (2, 'Alice'), (3, 'John')) AS T(uid, name)",
                        QueryProperty.FOREGROUND,
                        QueryProperty.SINGLE_SINK);

        assertThat(querySummary.getUdfCalls()).hasSize(1);
        assertThat(querySummary.getUdfCalls().iterator().next().getPath()).isEqualTo("`a`.`b`.`c`");
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

    /** Test function. */
    public static class MyScalarFunction extends ScalarFunction {
        public int eval(int num) {
            return num + 1;
        }
    }
}
