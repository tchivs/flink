/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.runtime.failure.DefaultFailureEnricherContext;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import io.confluent.flink.runtime.failure.util.FailureMessageUtil;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.flink.runtime.failure.FailureEnricherUtils.labelFailure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test that the TypeFailureEnricher correctly labels common exceptions in SQL queries. */
public class TypeFailureEnricherTableITCase extends StreamingTestBase {
    private static final int DELETED_SCHEMA_ID = 7;

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
    }

    @Test
    public void testArithmeticDivideByZeroError() {
        List<Row> sourceData =
                Arrays.asList(
                        Row.of(1, "1000", 2),
                        Row.of(2, "1", 0),
                        Row.of(3, "2000", 4),
                        Row.of(1, "2", 2),
                        Row.of(2, "3000", 3));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        String sourceDDL =
                "create table t1(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
        String sinkDDL =
                "create table t2(a int, b varchar, c int) with ('connector' = 'COLLECTION')";

        String query = "select t1.a, t1.b, t1.a / t1.c as d from t1";

        tEnv().executeSql(sourceDDL);
        tEnv().executeSql(sinkDDL);
        tEnv().executeSql(query);
        Table tableQuery = tEnv().sqlQuery(query);
        assertThatThrownBy(() -> tableQuery.executeInsert("t2").await())
                .satisfies(
                        e ->
                                assertFailureEnricherLabels(
                                        (Exception) e,
                                        "ERROR_CLASS_CODE",
                                        "3",
                                        "TYPE",
                                        "USER",
                                        "USER_ERROR_MSG",
                                        "ignore"));
    }

    @Test
    public void testNestedIOSerializationError() throws ExecutionException, InterruptedException {
        Exception userIOException =
                new IOException(
                        "Failed to deserialize consumer record due to",
                        new RuntimeException("test"));
        final String expectedUserMessage = FailureMessageUtil.buildMessage(userIOException);
        assertFailureEnricherLabels(
                userIOException,
                "ERROR_CLASS_CODE",
                "1",
                "TYPE",
                "USER",
                "USER_ERROR_MSG",
                expectedUserMessage);
    }

    @Test
    public void testSchemaNotFoundError() {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        assertThatThrownBy(() -> client.getSchemaById(DELETED_SCHEMA_ID))
                .isInstanceOf(RestClientException.class)
                .satisfies(
                        e ->
                                assertFailureEnricherLabels(
                                        (Exception) e,
                                        "ERROR_CLASS_CODE",
                                        "9",
                                        "TYPE",
                                        "USER",
                                        "USER_ERROR_MSG",
                                        "ignore"));
    }

    @Test
    public void testCastingError() {
        List<Row> sourceData =
                Arrays.asList(
                        Row.of(1, "122.145.8.244", 2),
                        Row.of(2, "1", 0),
                        Row.of(3, "2000", 4),
                        Row.of(1, "2", 2),
                        Row.of(2, "3000", 3));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        String sourceDDL =
                "create table t1(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
        String sinkDDL = "create table t2(a int, b int, c int) with ('connector' = 'COLLECTION')";

        String query = "select a, cast(`b` as int), c from t1;";

        tEnv().executeSql(sourceDDL);
        tEnv().executeSql(sinkDDL);
        tEnv().executeSql(query);
        Table tableQuery = tEnv().sqlQuery(query);
        TestCollectionTableFactory.initData(sourceData);
        assertThatThrownBy(() -> tableQuery.executeInsert("t2").await())
                .satisfies(
                        e ->
                                assertFailureEnricherLabels(
                                        (Exception) e,
                                        "ERROR_CLASS_CODE",
                                        "3",
                                        "TYPE",
                                        "USER",
                                        "USER_ERROR_MSG",
                                        "ignore"));
    }

    @Test
    public void testIncorrectSchemaSerializationError() {
        List<Row> sourceData =
                Arrays.asList(
                        Row.of(1, "1000", 2),
                        Row.of(2, "1", 0),
                        Row.of(3, "2000", 4),
                        Row.of(1, "2", 2),
                        Row.of(2, "3000", "3"));

        TestCollectionTableFactory.reset();

        String sourceDDL =
                "create table t1(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
        String sinkDDL =
                "create table t2(a int, b varchar, c int) with ('connector' = 'COLLECTION')";

        String query = "select t1.a, t1.b, t1.a / t1.c as d from t1";

        tEnv().executeSql(sourceDDL);
        tEnv().executeSql(sinkDDL);
        tEnv().executeSql(query);
        Table tableQuery = tEnv().sqlQuery(query);
        TestCollectionTableFactory.initData(sourceData);
        assertThatThrownBy(() -> tableQuery.executeInsert("t2").await())
                .satisfies(
                        e ->
                                assertFailureEnricherLabels(
                                        (Exception) e,
                                        "ERROR_CLASS_CODE",
                                        "1",
                                        "TYPE",
                                        "USER",
                                        "USER_ERROR_MSG",
                                        "ignore"));
    }

    public static void assertFailureEnricherLabels(Exception e, String... keyValuePairs)
            throws ExecutionException, InterruptedException {
        assertFailureEnricherLabels(new Configuration(), e, keyValuePairs);
    }

    public static void assertFailureEnricherLabels(
            Configuration configuration, Exception e, String... keyValuePairs)
            throws ExecutionException, InterruptedException {
        int numEntries = keyValuePairs.length / 2;
        Map<String, String> map = CollectionUtil.newHashMapWithExpectedSize(numEntries);

        for (int i = 0; i < numEntries; ++i) {
            map.put(keyValuePairs[2 * i], keyValuePairs[2 * i + 1]);
        }
        assertFailureEnricherLabels(configuration, e, map);
    }

    public static void assertFailureEnricherLabels(
            Configuration configuration, Exception e, Map<String, String> expectedLabels)
            throws ExecutionException, InterruptedException {
        final Context taskFailureCtx =
                DefaultFailureEnricherContext.forTaskFailure(
                        null, null, null, newSingleThreadExecutor(), null);
        final CompletableFuture<Map<String, String>> resultFuture =
                labelFailure(
                        e,
                        taskFailureCtx,
                        newSingleThreadExecutor(),
                        Collections.singleton(new TypeFailureEnricher(configuration)));
        Map<String, String> failureLabels = resultFuture.get();

        // Mechanism to ignore the actual user message.
        String keyUserMsg = "USER_ERROR_MSG";
        String valUserMsgIgnore = "ignore";
        if (valUserMsgIgnore.equalsIgnoreCase(expectedLabels.get(keyUserMsg))) {
            failureLabels = new HashMap<>(failureLabels);
            failureLabels.replace(keyUserMsg, valUserMsgIgnore);
        }
        assertThat(failureLabels).containsExactlyInAnyOrderEntriesOf(expectedLabels);
    }
}
