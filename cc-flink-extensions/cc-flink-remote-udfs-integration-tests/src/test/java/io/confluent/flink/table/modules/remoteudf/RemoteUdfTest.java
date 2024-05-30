/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.modules.remoteudf.mock.MockedFunctionWithTypes;
import io.confluent.flink.table.modules.remoteudf.util.TestUtils;
import io.confluent.flink.table.modules.remoteudf.utils.NamesGenerator;
import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import io.confluent.flink.table.service.ServiceTasks;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Basic Unit tests for remote UDF. */
@Confluent
public class RemoteUdfTest extends AbstractTestBase {

    private static final String TEST_ORG = String.format("org-%s", NamesGenerator.nextRandomName());
    private static final String TEST_ENV = String.format("env-%s", NamesGenerator.nextRandomName());
    private static MockedFunctionWithTypes[] testFunctionTriple =
            new MockedFunctionWithTypes[] {
                new MockedFunctionWithTypes(
                        "remote1",
                        ImmutableList.of(
                                new String[] {"INT", "STRING", "INT"}, new String[] {"INT"}),
                        ImmutableList.of("STRING", "INT")),
                new MockedFunctionWithTypes("remote2", new String[] {"STRING"}, "STRING"),
                new MockedFunctionWithTypes("remote3", new String[] {}, "STRING")
            };

    @Test
    public void testIsSmallerBuildInFunction() throws Exception {
        final RemoteUdfModule remoteUdfModule = new RemoteUdfModule();
        assertThat(remoteUdfModule.listFunctions().size()).isEqualTo(1);
        assertThat(remoteUdfModule.getFunctionDefinition("IS_SMALLER")).isPresent();

        final TableEnvironment tableEnv =
                TestUtils.getJssTableEnvironment(TEST_ORG, TEST_ENV, testFunctionTriple);
        final ForegroundJobResultPlan plan =
                foregroundJobCustomConfig(tableEnv, "SELECT IS_SMALLER('Small', 'Large')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testJssRemoteUdfsEnabled() throws Exception {
        // should be enabled by default for JSS service
        final TableEnvironment tableEnv =
                TestUtils.getJssTableEnvironment(TEST_ORG, TEST_ENV, testFunctionTriple);

        final ForegroundJobResultPlan plan =
                foregroundJobCustomConfig(tableEnv, "SELECT cat1.db1.remote2('payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testJssRemoteUdfsNoExpressionReducer() throws Exception {
        // should be enabled by default for JSS service
        final TableEnvironment tableEnv =
                TestUtils.getJssTableEnvironment(TEST_ORG, TEST_ENV, testFunctionTriple);

        final ForegroundJobResultPlan plan =
                foregroundJobCustomConfig(tableEnv, "SELECT cat1.db1.remote1(1)");
        // The value of remote(1) is INT_RETURN_VALUE, but we want to ensure that the
        // Expression reducer didn't inline the value into the plan.
        assertThat(plan.getCompiledPlan())
                .doesNotContain(Integer.toString(TestUtils.EXPECTED_INT_RETURN_VALUE));
    }

    @Test
    public void testRemoteUdfsEnabled() throws Exception {
        // SQL service controls remote UDFs using config params
        final TableEnvironment tableEnv =
                TestUtils.getSqlServiceTableEnvironment(
                        TEST_ORG, TEST_ENV, testFunctionTriple, true, false);

        final ForegroundJobResultPlan plan =
                foregroundJobCustomConfig(tableEnv, "SELECT cat1.db1.remote2('payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    /* This is important to check otherwise a '-' char in the identifier would fail the job*/
    @Test
    public void testEscapingIdentifiersForCatalogDatabaseNames() throws Exception {
        MockedFunctionWithTypes[] testFunctionWithHyphen =
                new MockedFunctionWithTypes[] {
                    new MockedFunctionWithTypes("remote-4", new String[] {"INT"}, "STRING")
                };
        // SQL service controls remote UDFs using config params
        final TableEnvironment tableEnv =
                TestUtils.getSqlServiceTableEnvironment(
                        TEST_ORG, TEST_ENV, testFunctionWithHyphen, true, false);

        final ForegroundJobResultPlan plan =
                foregroundJobCustomConfig(tableEnv, "SELECT cat1.db1.`remote-4`(1)");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testRemoteUdfsEnabled_useCatalogDb() throws Exception {
        // SQL service controls remote UDFs using config params
        final TableEnvironment tableEnv =
                TestUtils.getSqlServiceTableEnvironment(
                        TEST_ORG, TEST_ENV, testFunctionTriple, true, true);
        tableEnv.executeSql("USE CATALOG cat1");
        tableEnv.executeSql("USE db1");
        final ForegroundJobResultPlan plan =
                foregroundJobCustomConfig(tableEnv, "SELECT remote2('payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testRemoteUdfsDisabled() {
        final TableEnvironment tableEnv =
                TestUtils.getSqlServiceTableEnvironment(
                        TEST_ORG, TEST_ENV, testFunctionTriple, false, false);
        assertThatThrownBy(() -> tableEnv.executeSql("SELECT remote2('payload')"))
                .message()
                .contains("No match found for function signature remote2");
    }

    public static ForegroundJobResultPlan foregroundJobCustomConfig(
            TableEnvironment tableEnv, String sql) throws Exception {
        final QueryOperation queryOperation = tableEnv.sqlQuery(sql).getQueryOperation();
        return (ForegroundJobResultPlan)
                ServiceTasks.INSTANCE.compileForegroundQuery(
                        tableEnv,
                        queryOperation,
                        (identifier, execNodeId, tableOptions) -> Collections.emptyMap());
    }
}
