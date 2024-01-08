/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.compute.credentials.ComputePoolKeyCacheImpl;
import io.confluent.flink.compute.credentials.InMemoryCredentialDecrypterImpl;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.service.ResultPlanUtils;
import io.confluent.flink.table.service.ServiceTasks;
import io.confluent.flink.table.service.ServiceTasks.Service;
import io.confluent.flink.table.service.ServiceTasksOptions;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.MGF1ParameterSpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_AI_FUNCTIONS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Confluent AI UDFs. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class ConfluentAIFunctionsITCase extends AbstractTestBase {

    private static final int PARALLELISM = 4;
    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    private final MockWebServer mockOpenAiWebServer = new MockWebServer();
    private MockResponse mockOpenAiResponse;
    private KeyPair keyPair;

    @BeforeEach
    public void before() throws NoSuchAlgorithmException, IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        keyPair = createKeyPair();
        mockOpenAiWebServer.start();
        mockOpenAiResponse =
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
                                        + "}");
    }

    private static TableEnvironment getSqlServiceTableEnvironment(boolean aiFunctionsEnabled) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.singletonMap(
                        CONFLUENT_AI_FUNCTIONS_ENABLED.key(), String.valueOf(aiFunctionsEnabled)),
                Service.SQL_SERVICE);
        return tableEnv;
    }

    private static TableEnvironment getJssTableEnvironment() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Service.JOB_SUBMISSION_SERVICE);
        return tableEnv;
    }

    @Test
    public void testNumberOfBuiltinFunctions() {
        final AIFunctionsModule aiFunctionsModule = new AIFunctionsModule(Collections.emptyMap());
        assertThat(aiFunctionsModule.listFunctions().size()).isEqualTo(2);
        assertThat(aiFunctionsModule.getFunctionDefinition("INVOKE_OPENAI")).isPresent();
        assertThat(aiFunctionsModule.getFunctionDefinition("SECRET")).isPresent();
    }

    @Test
    public void testJssAIFunctionEnabled() throws Exception {
        // should be enabled by default for JSS service
        final TableEnvironment tableEnv = getJssTableEnvironment();

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJobCustomConfig(tableEnv, "SELECT SECRET('something')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testAIFunctionEnabled() throws Exception {
        // SQL service controls AI functions using config params
        final TableEnvironment tableEnv = getSqlServiceTableEnvironment(true);

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJobCustomConfig(tableEnv, "SELECT SECRET('something')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testAIFunctionDisabled() {
        final TableEnvironment tableEnv = getSqlServiceTableEnvironment(false);

        assertThatThrownBy(() -> tableEnv.executeSql("SELECT SECRET('a', 'b');"))
                .hasMessageContaining("No match found for function signature SECRET");
    }

    @Test
    public void testAIFunctionSecretUDF() throws Exception {
        final String apiKey = "someOpenAIKey";
        final List<Row> expectedRows = Collections.singletonList(Row.of(apiKey));

        new EnvironmentVariables("OPENAI_API_KEY", apiKey)
                .execute(
                        () -> {
                            // in here the environment is temporarily set
                            TableEnvironment tEnv = getSqlServiceTableEnvironment(true);
                            TableResult result = tEnv.executeSql("SELECT SECRET();");
                            final List<Row> results = new ArrayList<>();
                            result.collect().forEachRemaining(results::add);
                            assertThat(results).containsExactlyInAnyOrderElementsOf(expectedRows);
                        });
    }

    @Test
    public void testSecretUDFNoApiKeySet() {
        final TableEnvironment tEnv = getSqlServiceTableEnvironment(true);

        assertThatThrownBy(
                        () -> {
                            TableResult result = tEnv.executeSql("SELECT SECRET();");
                            result.collect().forEachRemaining(System.out::println);
                        })
                .hasStackTraceContaining("OPENAI_API_KEY");
    }

    @Test
    public void testSecretUDFNoSecretSet() {
        final HttpUrl baseUrl = mockOpenAiWebServer.url("/v1/chat/completions");
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.loadModule(
                "testOpenAi",
                new AIFunctionsTestModule(
                        baseUrl.toString(),
                        new MockedInMemoryCredentialDecrypterImpl(null),
                        Collections.emptyMap()));

        assertThatThrownBy(
                        () -> {
                            TableResult result =
                                    tEnv.executeSql("SELECT SECRET('unknownSecretName');");
                            result.collect().forEachRemaining(System.out::println);
                        })
                .hasStackTraceContaining(
                        "SECRET is null. Please SET 'unknownSecretName' and resubmit job.");
    }

    @Test
    public void testAIFunctionAiGenerateWithMockedSecretUDF() throws Exception {
        final HttpUrl baseUrl = mockOpenAiWebServer.url("/v1/chat/completions");
        // value parse from JSON message.content
        final List<Row> expectedRows = Collections.singletonList(Row.of("4"));
        // mock openAI completions JSON response
        mockOpenAiWebServer.enqueue(mockOpenAiResponse);
        final String base64EncryptedSecret =
                Base64.getEncoder()
                        .encodeToString(encryptMessage("someApiKeyValue", keyPair.getPublic()));
        // Mock AIResponseGenerator baseURL to make sure we control responses
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // Setup private Conf with SQL_SECRETS
        final Configuration privateConfig =
                Configuration.fromMap(
                        Collections.singletonMap(
                                String.format(
                                        "%s.%s",
                                        ServiceTasksOptions.SQL_SECRETS.key(), "someApiKey"),
                                base64EncryptedSecret));

        tableEnv.loadModule(
                "testOpenAi",
                new AIFunctionsTestModule(
                        baseUrl.toString(),
                        new MockedInMemoryCredentialDecrypterImpl(
                                keyPair.getPrivate().getEncoded()),
                        privateConfig.get(ServiceTasksOptions.SQL_SECRETS)));

        // test INVOKE_OPENAI with SECRET UDF
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT INVOKE_OPENAI('Take the following text and score it from happy to sad, outputting a 0 to 10 numeric scale.  Respond only with the numeric score.', 'Im feeling a little down', SECRET('someApiKey'));");
        final List<Row> results = new ArrayList<>();
        result.collect().forEachRemaining(results::add);
        assertThat(results).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    @Test
    public void testAIFunctionAiGenerateWithSecretUDFPropagation() throws Exception {
        final HttpUrl baseUrl = mockOpenAiWebServer.url("/v1/chat/completions");
        mockOpenAiWebServer.enqueue(mockOpenAiResponse);
        // value parse from JSON message.content
        final List<Row> expectedRows = Collections.singletonList(Row.of("4"));
        final String base64EncryptedSecret =
                Base64.getEncoder()
                        .encodeToString(encryptMessage("someUserSecretValue", keyPair.getPublic()));
        // mock AIResponseGenerator baseURL to make sure we control responses
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // Setup private Conf with SQL_SECRETS
        final Configuration privateConfig =
                Configuration.fromMap(
                        Collections.singletonMap(
                                String.format(
                                        "%s.%s",
                                        ServiceTasksOptions.SQL_SECRETS.key(),
                                        "someUserSecretName"),
                                base64EncryptedSecret));

        tableEnv.loadModule(
                "testOpenAi",
                new AIFunctionsTestModule(
                        baseUrl.toString(),
                        InMemoryCredentialDecrypterImpl.INSTANCE,
                        privateConfig.get(ServiceTasksOptions.SQL_SECRETS)));

        // test INVOKE_OPENAI with SET property passing down encrypted Secret
        final AtomicBoolean isPropagatorDone = new AtomicBoolean(false);
        secretExecPropagator(isPropagatorDone);

        final TableResult setResult =
                tableEnv.executeSql(
                        "SELECT INVOKE_OPENAI('Take the following text and score it from happy to sad, outputting a 0 to 10 numeric scale.  Respond only with the numeric score.', 'Im feeling a little down', SECRET('someUserSecretName'));");
        final List<Row> setResults = new ArrayList<>();
        setResult.collect().forEachRemaining(setResults::add);
        assertThat(setResults).containsExactlyInAnyOrderElementsOf(expectedRows);
        // Shutting down Executor service
        isPropagatorDone.set(true);
    }

    private void secretExecPropagator(AtomicBoolean isDone) {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(
                () -> {
                    if (!isDone.get()) {
                        ComputePoolKeyCacheImpl.INSTANCE.onNewPrivateKeyObtained(
                                keyPair.getPrivate().getEncoded());
                    } else {
                        executorService.shutdownNow();
                    }
                },
                0,
                100,
                TimeUnit.MILLISECONDS);
    }

    private static KeyPair createKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.generateKeyPair();
    }

    private static byte[] encryptMessage(String message, java.security.PublicKey pubKey)
            throws Exception {
        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPPadding");
        OAEPParameterSpec oaepParams =
                new OAEPParameterSpec(
                        "SHA-256",
                        "MGF1",
                        new MGF1ParameterSpec("SHA-256"),
                        PSource.PSpecified.DEFAULT);
        cipher.init(Cipher.ENCRYPT_MODE, pubKey, oaepParams);
        return cipher.doFinal(message.getBytes(StandardCharsets.UTF_8));
    }

    private static class MockedInMemoryCredentialDecrypterImpl
            extends InMemoryCredentialDecrypterImpl {
        final byte[] privateKey;

        MockedInMemoryCredentialDecrypterImpl(byte[] privateKey) {
            this.privateKey = privateKey;
        }

        @Override
        protected byte[] readPrivateKey() {
            return privateKey;
        }
    }
}
