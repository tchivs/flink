/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Gets the static credentials and does a token exchange for a DPAT token from cc-flow-service and
 * cc-auth-service via cc-gateway-service.
 */
@Confluent
public class TokenExchangerImpl implements TokenExchanger {
    private static final String PATH = "/api/flink/access_tokens";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    public static final MediaType JSON = MediaType.get("application/json");

    private final String gatewayServiceServer;
    private final OkHttpClient httpClient;

    /**
     * Creates a token fetcher.
     *
     * @param gatewayServiceServer The server, e.g. http://hostname:port
     */
    public TokenExchangerImpl(String gatewayServiceServer) {
        this.gatewayServiceServer = gatewayServiceServer;
        this.httpClient = new OkHttpClient.Builder().build();
    }

    @VisibleForTesting
    protected TokenExchangerImpl(String gatewayServiceServer, OkHttpClient httpClient) {
        this.gatewayServiceServer = gatewayServiceServer;
        this.httpClient = httpClient;
    }

    public DPATToken fetch(
            Pair<String, String> staticCredentials, JobCredentialsMetadata jobCredentialsMetadata) {
        URL url = null;
        try {
            url = new URL(gatewayServiceServer + PATH);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Badly configured server: " + gatewayServiceServer);
        }

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("statement_id", jobCredentialsMetadata.getStatementIdCRN());
        node.put("compute_pool_id", jobCredentialsMetadata.getComputePoolId());

        RequestBody body = RequestBody.create(JSON, node.toString());
        String credential =
                Credentials.basic(staticCredentials.getKey(), staticCredentials.getValue());
        Request request =
                new Request.Builder()
                        .url(url)
                        .post(body)
                        .header(AUTHORIZATION_HEADER, credential)
                        .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                return new DPATToken(getTokenFromResponse(mapper, response.body().string()));
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
            throw new FlinkRuntimeException("Failed to fetch token", e);
        }
    }

    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    @VisibleForTesting
    static String getTokenFromResponse(ObjectMapper mapper, String jsonResponse)
            throws JsonProcessingException {
        JsonNode jsonNode = mapper.readTree(jsonResponse);
        JsonNode tokenNode = jsonNode.get("token");
        JsonNode errorNode = jsonNode.get("error");
        if (tokenNode == null || tokenNode.isNull() || !tokenNode.isTextual()) {
            String message =
                    errorNode != null && errorNode.isTextual()
                            ? errorNode.asText()
                            : "<no message>";
            throw new FlinkRuntimeException("Error fetching token: " + message);
        }
        return tokenNode.asText();
    }
}
