/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.functions.ai;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.functions.scalar.ai.AIResponseGenerator;
import io.confluent.flink.table.functions.scalar.ai.AISecret;
import io.confluent.flink.table.service.ForegroundResultPlan;
import io.confluent.flink.table.service.ServiceTasks;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_AI_FUNCTIONS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Confluent AI UDFs: {@link AISecret}. {@link AIResponseGenerator}. */
@Confluent
public class ConfluentAIFunctionsITCase extends AbstractTestBase {

    private static final int PARALLELISM = 4;
    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    private StreamExecutionEnvironment env;
    private final MockWebServer mockWebServer = new MockWebServer();

    @BeforeEach
    public void before() {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(PARALLELISM);
    }

    private static TableEnvironment getTableEnvironment(boolean aiFunctionsEnabled) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnv.getConfig()
                .set(CONFLUENT_AI_FUNCTIONS_ENABLED.key(), String.valueOf(aiFunctionsEnabled));
        INSTANCE.configureEnvironment(tableEnv, Collections.emptyMap(), true);
        return tableEnv;
    }

    @Test
    public void testAIFunctionEnabled() throws Exception {
        TableEnvironment tableEnv = getTableEnvironment(true);

        final QueryOperation queryOperation =
                tableEnv.sqlQuery("SELECT SECRET(\'something\');").getQueryOperation();

        final ForegroundResultPlan plan =
                INSTANCE.compileForegroundQuery(
                        tableEnv,
                        queryOperation,
                        (identifier, execNodeId) -> Collections.emptyMap());
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testAIFunctionDisabled() {
        TableEnvironment tableEnv = getTableEnvironment(false);

        assertThatThrownBy(() -> tableEnv.executeSql("SELECT SECRET(\'a\', \'b\');"))
                .satisfies(
                        e ->
                                e.getMessage()
                                        .contains("No match found for function signature SECRET"));
    }

    @Test
    public void testAIFunctionSecretUDF() throws Exception {
        final String apiKey = "someOpenAIKey";
        final List<Row> expectedRows = Arrays.asList(Row.of(apiKey));

        new EnvironmentVariables("OPENAI_API_KEY", apiKey)
                .execute(
                        () -> {
                            // in here the environment is temporarily set
                            TableEnvironment tEnv = getTableEnvironment(true);
                            TableResult result = tEnv.executeSql("SELECT SECRET(\'a\', \'b\');");
                            final List<Row> results = new ArrayList<>();
                            result.collect().forEachRemaining(results::add);
                            assertThat(results).containsExactlyInAnyOrderElementsOf(expectedRows);
                        });
    }

    @Test
    public void testSecretUDFNoApiKeySet() {
        TableEnvironment tEnv = getTableEnvironment(true);

        assertThatThrownBy(
                        () -> {
                            TableResult result = tEnv.executeSql("SELECT SECRET(\'a\', \'b\');");
                            final List<Row> results = new ArrayList<>();
                            result.collect().forEachRemaining(results::add);
                        })
                .satisfies(e -> e.getMessage().contains("OPENAI_API_KEY"));
    }

    @Test
    public void testAIFunctionAiGenerateUDF() throws Exception {
        this.mockWebServer.start();
        HttpUrl baseUrl = mockWebServer.url("/v1/chat/completions");
        // value parse from JSON message.content
        final List<Row> expectedRows = Arrays.asList(Row.of("4"));
        // mock openAI completions JSON response
        mockWebServer.enqueue(
                new MockResponse()
                        .addHeader("Content-Type", "application/json; charset=utf-8")
                        .setBody(
                                "{\n"
                                        + "  \"id\": \"chatcmpl-7eU7uxxydeuJP1HOcLchRPVTKe15x\",\n"
                                        + "  \"object\": \"chat.completion\",\n"
                                        + "  \"created\": 1689883282,\n"
                                        + "  \"model\": \"gpt-3.5-turbo-0613\",\n"
                                        + "  \"choices\": [\n"
                                        + "    {\n"
                                        + "      \"index\": 0,\n"
                                        + "      \"message\": {\n"
                                        + "        \"role\": \"assistant\",\n"
                                        + "        \"content\": \"4\"\n"
                                        + "      },\n"
                                        + "      \"finish_reason\": \"stop\"\n"
                                        + "    }\n"
                                        + "  ],\n"
                                        + "  \"usage\": {\n"
                                        + "    \"prompt_tokens\": 47,\n"
                                        + "    \"completion_tokens\": 1,\n"
                                        + "    \"total_tokens\": 48\n"
                                        + "  }\n"
                                        + "}"));
        TableEnvironment tEnv = getTableEnvironment(true);

        TableResult result =
                tEnv.executeSql(
                        "SELECT AI_GENERATE('"
                                + baseUrl.toString()
                                + "', 'Take the following text and score it from happy to sad, outputting a 0 to 10 numeric scale.  Respond only with the numeric score.', 'Im feeling a little down', 'someApiKey');");
        final List<Row> results = new ArrayList<>();
        result.collect().forEachRemaining(results::add);
        assertThat(results).containsExactlyInAnyOrderElementsOf(expectedRows);
    }
}
