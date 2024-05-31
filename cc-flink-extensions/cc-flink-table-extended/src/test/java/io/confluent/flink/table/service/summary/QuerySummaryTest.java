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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.ScalarFunction;

import io.confluent.flink.table.modules.remoteudf.ConfiguredFunctionSpec;
import io.confluent.flink.table.modules.remoteudf.ConfiguredRemoteScalarFunction;
import io.confluent.flink.table.service.BackgroundJobResultPlan;
import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import io.confluent.flink.table.service.ResultPlanUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_ASYNC_ENABLED;
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
                    .addArgumentTypes(Arrays.asList("INT", "INT"))
                    .addArgumentTypes(Collections.singletonList("STRING"))
                    .addReturnType("INT")
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

        createTable(env, "bounded1", Boundedness.BOUNDED, false);
        createTable(env, "bounded2", Boundedness.BOUNDED, false);
        createTable(env, "unbounded", Boundedness.CONTINUOUS_UNBOUNDED, false);

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

        createTable(env, "unbounded", Boundedness.CONTINUOUS_UNBOUNDED, false);

        assertProperties(env, "SELECT * FROM unbounded", QueryProperty.APPEND_ONLY);
        assertProperties(env, "SELECT COUNT(*) FROM unbounded", QueryProperty.UPDATING);
    }

    @Test
    void testChangelogModeForPointInTime() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        createTable(env, "bounded1", Boundedness.BOUNDED, false);

        assertProperties(env, "SELECT * FROM bounded1", QueryProperty.APPEND_ONLY);
        assertProperties(env, "SELECT COUNT(*) FROM bounded1", QueryProperty.APPEND_ONLY);
    }

    @Test
    void testSourceAndSinkIdentifierTags() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        createTable(env, "unbounded_source", Boundedness.CONTINUOUS_UNBOUNDED, false);
        createTableSink(env, "unbounded_sink");

        QuerySummary querySummary =
                assertPropertiesBackgroundQuery(
                        env,
                        "INSERT INTO unbounded_sink SELECT * FROM unbounded_source",
                        QueryProperty.APPEND_ONLY,
                        QueryProperty.BACKGROUND);

        final NodeSummary sinkSummary = querySummary.getNodes().get(0);
        assertThat(sinkSummary).extracting(NodeSummary::getKind).isEqualTo(NodeKind.SINK);
        assertThat(sinkSummary.getTag(NodeTag.TABLE_IDENTIFIER, ObjectIdentifier.class))
                .isNotNull()
                .isEqualTo(
                        ObjectIdentifier.of(
                                "default_catalog", "default_database", "unbounded_sink"));

        final NodeSummary sourceSummary = sinkSummary.getInputs().get(0);
        assertThat(sourceSummary).extracting(NodeSummary::getKind).isEqualTo(NodeKind.SOURCE_SCAN);
        assertThat(sourceSummary.getTag(NodeTag.TABLE_IDENTIFIER, ObjectIdentifier.class))
                .isNotNull()
                .isEqualTo(
                        ObjectIdentifier.of(
                                "default_catalog", "default_database", "unbounded_source"));
    }

    @Test
    void testUpsertKeys() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        createTable(env, "unbounded1", Boundedness.CONTINUOUS_UNBOUNDED, false);
        createTable(env, "unbounded2", Boundedness.CONTINUOUS_UNBOUNDED, false);

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

    @Test
    void testSummaryAsyncUdf() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporaryFunction(
                "a.b.c",
                new ConfiguredRemoteScalarFunction(
                        Collections.singletonMap(CONFLUENT_REMOTE_UDF_ASYNC_ENABLED.key(), "true"),
                        UDF_SPECS));

        env.createTemporaryFunction(
                "a.b.d",
                new ConfiguredRemoteScalarFunction(
                        Collections.singletonMap(CONFLUENT_REMOTE_UDF_ASYNC_ENABLED.key(), "false"),
                        UDF_SPECS2));

        final QuerySummary querySummary =
                assertProperties(
                        env,
                        "SELECT a.b.c(uid), a.b.d(uid), LOWER(name) "
                                + "FROM (VALUES (1, 'Bob'), (2, 'Alice'), (3, 'John')) AS T(uid, name)",
                        QueryProperty.FOREGROUND,
                        QueryProperty.SINGLE_SINK);

        assertThat(querySummary.getNodeKinds())
                .containsExactlyInAnyOrder(
                        NodeKind.VALUES, NodeKind.ASYNC_CALC, NodeKind.CALC, NodeKind.SINK);

        assertThat(querySummary.getExpressionKinds())
                .containsExactlyInAnyOrder(ExpressionKind.INPUT_REF, ExpressionKind.OTHER);

        assertThat(querySummary.getUdfCalls()).hasSize(2);
        List<String> names =
                querySummary.getUdfCalls().stream()
                        .map(call -> call.getPath())
                        .collect(Collectors.toList());
        assertThat(names).containsExactlyInAnyOrder("`a`.`b`.`c`", "`a`.`b`.`d`");

        final NodeSummary sinkSummary = querySummary.getNodes().get(0);
        assertThat(sinkSummary).extracting(NodeSummary::getKind).isEqualTo(NodeKind.SINK);

        final NodeSummary calcSummary = sinkSummary.getInputs().get(0);
        assertThat(calcSummary).extracting(NodeSummary::getKind).isEqualTo(NodeKind.CALC);
        assertThat(calcSummary.getTag(NodeTag.EXPRESSIONS, Set.class)).isNotNull().hasSize(2);
        final NodeSummary asyncCalcSummary = calcSummary.getInputs().get(0);
        assertThat(asyncCalcSummary)
                .extracting(NodeSummary::getKind)
                .isEqualTo(NodeKind.ASYNC_CALC);
        assertThat(asyncCalcSummary.getTag(NodeTag.EXPRESSIONS, Set.class)).isNotNull().hasSize(2);
    }

    @Test
    void testSummaryUdfJoin() throws Exception {
        testUdfQuery(
                "SELECT * "
                        + "FROM bounded1 x RIGHT JOIN bounded2 y "
                        + "ON x.k=y.k where "
                        + " x.k=y.k and a.b.c(x.v, y.v) > 10");
    }

    @Test
    void testSummaryUdfWindowJoin() throws Exception {
        testUdfQuery(
                "SELECT * "
                        + "FROM ("
                        + "  SELECT * FROM TABLE(TUMBLE(TABLE bounded1, DESCRIPTOR(t), INTERVAL '5' MINUTES))"
                        + ") L "
                        + " FULL JOIN ("
                        + "  SELECT * FROM TABLE(TUMBLE(TABLE bounded2, DESCRIPTOR(t), INTERVAL '5' MINUTES))"
                        + ") R "
                        + "ON L.k = R.k AND L.window_start = R.window_start AND L.window_end = R.window_end "
                        + "AND a.b.c(L.v, R.v) > 10");
    }

    @Test
    void testSummaryUdfTemporalJoin() throws Exception {
        testUdfQuery(
                "SELECT * "
                        + "FROM bounded1 JOIN bounded2 FOR SYSTEM_TIME AS OF bounded1.t "
                        + "ON bounded1.k=bounded2.k where "
                        + " a.b.c(bounded1.v, bounded2.v) > 10");
    }

    private static void testUdfQuery(String sqlQuery) throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporaryFunction(
                "a.b.c",
                new ConfiguredRemoteScalarFunction(
                        Collections.singletonMap(CONFLUENT_REMOTE_UDF_ASYNC_ENABLED.key(), "false"),
                        UDF_SPECS));

        createTable(env, "bounded1", Boundedness.BOUNDED, true);
        createTable(env, "bounded2", Boundedness.BOUNDED, true);

        final QuerySummary querySummary =
                assertProperties(
                        env, sqlQuery, QueryProperty.FOREGROUND, QueryProperty.SINGLE_SINK);

        assertThat(querySummary.getUdfCalls()).hasSize(1);
        assertThat(querySummary.getUdfCalls().iterator().next().getPath()).isEqualTo("`a`.`b`.`c`");
        assertThat(querySummary.getUdfCalls().iterator().next().getResourceConfigs().size())
                .isGreaterThan(0);
    }

    private static void createConfluentCatalogTable(
            TableEnvironment env, String name, Map<String, String> options, boolean rowTime)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("k", DataTypes.INT().notNull())
                        .column("v", DataTypes.INT())
                        .column("t", DataTypes.TIMESTAMP(3))
                        .primaryKey("k");
        if (rowTime) {
            builder.watermark("t", "t - INTERVAL '5' SECOND");
        }
        ResultPlanUtils.createConfluentCatalogTable(env, name, builder.build(), options);
    }

    private static void createTable(
            TableEnvironment env, String name, Boundedness boundedness, boolean rowTime)
            throws Exception {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "datagen");
        if (boundedness == Boundedness.BOUNDED) {
            options.put("number-of-rows", "10");
        }
        createConfluentCatalogTable(env, name, options, rowTime);
    }

    private static void createTableSink(TableEnvironment env, String name) throws Exception {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "blackhole");
        createConfluentCatalogTable(env, name, options, false);
    }

    private static QuerySummary assertProperties(
            TableEnvironment env, String sql, QueryProperty... properties) throws Exception {
        final ForegroundJobResultPlan plan = ResultPlanUtils.foregroundJob(env, sql);
        final QuerySummary querySummary = plan.getQuerySummary();
        assertThat(querySummary.getProperties()).contains(properties);
        return querySummary;
    }

    private static QuerySummary assertPropertiesBackgroundQuery(
            TableEnvironment env, String sql, QueryProperty... properties) throws Exception {
        final BackgroundJobResultPlan plan = ResultPlanUtils.backgroundJob(env, sql);
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
