/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.service.ServiceTasksOptions;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Class implementing aiGenerate function by calling OpenAI API. */
public class AIResponseGenerator extends AsyncScalarFunction {

    public static final String NAME = "INVOKE_OPENAI";

    private static final String openAICompletionsURL = "https://api.openai.com/v1/chat/completions";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    public static final MediaType JSON = MediaType.get("application/json");
    private final Configuration aiFunctionsConfig;
    private URL baseURL;
    private transient OkHttpClient httpClient;
    private transient ObjectMapper mapper;

    public AIResponseGenerator(Configuration aiFunctionsConfig) {
        this.aiFunctionsConfig = aiFunctionsConfig;
        setBaseUrl(openAICompletionsURL);
    }

    @VisibleForTesting
    public AIResponseGenerator(Configuration aiFunctionsConfig, String baseUrl) {
        this.aiFunctionsConfig = aiFunctionsConfig;
        setBaseUrl(baseUrl);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        long callTimeout =
                aiFunctionsConfig
                        .get(ServiceTasksOptions.CONFLUENT_AI_FUNCTIONS_CALL_TIMEOUT)
                        .toMillis();
        this.httpClient =
                new OkHttpClient.Builder()
                        .readTimeout(callTimeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(callTimeout, TimeUnit.MILLISECONDS)
                        .connectTimeout(callTimeout, TimeUnit.MILLISECONDS)
                        .callTimeout(callTimeout, TimeUnit.MILLISECONDS)
                        .build();
        this.mapper = new ObjectMapper();
    }

    public void close() {
        this.httpClient.dispatcher().executorService().shutdown();
        this.httpClient.connectionPool().evictAll();
    }

    public void eval(
            CompletableFuture<String> future,
            @Nullable String prompt,
            @Nullable String input,
            String apiKey) {
        if (prompt == null || input == null) {
            future.complete(null);
            return;
        }
        final ObjectNode node = mapper.createObjectNode();
        node.put("model", "gpt-3.5-turbo");
        node.put("temperature", 0.7);
        final ArrayNode arrayNode = node.putArray("messages");
        final ObjectNode messageSystem = arrayNode.addObject();
        messageSystem.put("role", "system");
        messageSystem.put("content", prompt);
        final ObjectNode messageUser = arrayNode.addObject();
        messageUser.put("role", "user");
        messageUser.put("content", input);

        final RequestBody body = RequestBody.create(JSON, node.toString());
        final Request request =
                new Request.Builder()
                        .url(baseURL)
                        .post(body)
                        .header(AUTHORIZATION_HEADER, "Bearer " + apiKey)
                        .build();
        httpClient
                .newCall(request)
                .enqueue(
                        new Callback() {
                            @Override
                            public void onFailure(Call call, IOException e) {
                                future.completeExceptionally(e);
                            }

                            @Override
                            public void onResponse(Call call, Response response)
                                    throws IOException {
                                try (ResponseBody responseBody = response.body()) {
                                    if (response.isSuccessful()) {
                                        future.complete(
                                                getContentFromResponse(
                                                        mapper,
                                                        responseBody != null
                                                                ? responseBody.string()
                                                                : ""));
                                    } else {
                                        throw new FlinkRuntimeException(
                                                String.format(
                                                        "Received bad response code %d message %s",
                                                        response.code(),
                                                        Strings.isNullOrEmpty(response.message())
                                                                        && responseBody != null
                                                                ? responseBody.string().trim()
                                                                : response.message()));
                                    }
                                } catch (Throwable t) {
                                    future.completeExceptionally(t);
                                }
                            }
                        });
    }

    @VisibleForTesting
    static String getContentFromResponse(ObjectMapper mapper, String jsonResponse)
            throws JsonProcessingException {
        final JsonNode jsonNode = mapper.readTree(jsonResponse);
        final JsonNode choicesNode = jsonNode.get("choices");
        if (choicesNode == null || choicesNode.isNull() || !choicesNode.isArray()) {
            throw new FlinkRuntimeException("Error fetching token: " + jsonResponse);
        }
        final List<JsonNode> choices = ImmutableList.copyOf(choicesNode.elements());
        if (choices.isEmpty()) {
            throw new FlinkRuntimeException("Empty choices: " + jsonResponse);
        }
        final JsonNode choiceNode = choices.get(0);
        final JsonNode messageNode = choiceNode.get("message");
        final JsonNode contentNode = messageNode.get("content");
        if (contentNode == null || contentNode.isNull() || !contentNode.isTextual()) {
            throw new FlinkRuntimeException("Didn't get proper content response: " + jsonResponse);
        }
        return contentNode.textValue();
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    private void setBaseUrl(String baseURL) {
        try {
            this.baseURL = new URL(baseURL);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Badly configured server: " + openAICompletionsURL);
        }
    }
}
