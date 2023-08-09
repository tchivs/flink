/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.functions.scalar.ai;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/** Class implementing aiGenerate function by calling OpenAI API. */
public class AIResponseGenerator extends ScalarFunction {

    private static final String openAICompletionsURL = "https://api.openai.com/v1/chat/completions";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    public static final MediaType JSON = MediaType.get("application/json");
    private URL baseURL;
    private transient OkHttpClient httpClient;
    private transient ObjectMapper mapper;

    public AIResponseGenerator() {
        setBaseUrl(openAICompletionsURL);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.httpClient = new OkHttpClient.Builder().build();
        this.mapper = new ObjectMapper();
    }

    public void setBaseUrl(String baseURL) {
        try {
            this.baseURL = new URL(baseURL);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Badly configured server: " + openAICompletionsURL);
        }
    }

    public @Nullable String eval(
            String baseUrl, @Nullable String prompt, @Nullable String input, String apiKey) {
        setBaseUrl(baseUrl);
        return eval(prompt, input, apiKey);
    }

    public @Nullable String eval(@Nullable String prompt, @Nullable String input, String apiKey) {
        if (prompt == null || input == null) {
            return null;
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
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                return getContentFromResponse(mapper, response.body().string());
            } else {
                throw new FlinkRuntimeException(
                        String.format(
                                "Received bad response code %d message %s",
                                response.code(),
                                Strings.isNullOrEmpty(response.message())
                                        ? response.body().string().trim()
                                        : response.message()));
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to fetch OpenAI Response", e);
        } finally {
            this.httpClient.dispatcher().executorService().shutdown();
            this.httpClient.connectionPool().evictAll();
        }
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
        if (choices.size() == 0) {
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
}
