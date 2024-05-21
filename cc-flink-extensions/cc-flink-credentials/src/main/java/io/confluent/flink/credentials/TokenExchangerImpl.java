/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.confluent.flink.credentials.TokenExchangerUtil.getIdentityPoolPrincipals;
import static io.confluent.flink.credentials.TokenExchangerUtil.getServiceAccountPrincipal;
import static io.confluent.flink.credentials.TokenExchangerUtil.getUserPrincipal;

/**
 * Gets the static credentials and does a token exchange for a DPAT token from cc-flow-service and
 * cc-auth-service via cc-gateway-service.
 */
@Confluent
public class TokenExchangerImpl implements TokenExchanger {

    private static final Logger LOG = LoggerFactory.getLogger(TokenExchangerImpl.class);
    private static final String PATH = "/flink/access_tokens";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    public static final MediaType JSON = MediaType.get("application/json");

    private final String gatewayServiceServer;
    private final OkHttpClient httpClient;

    /**
     * Creates a token fetcher.
     *
     * @param gatewayServiceServer The server, e.g. http://hostname:port
     */
    public TokenExchangerImpl(String gatewayServiceServer, long tokenExchangeTimeoutMs) {
        this.gatewayServiceServer = gatewayServiceServer;
        this.httpClient =
                new OkHttpClient.Builder()
                        .callTimeout(tokenExchangeTimeoutMs, TimeUnit.MILLISECONDS)
                        .build();
    }

    @VisibleForTesting
    protected TokenExchangerImpl(String gatewayServiceServer, OkHttpClient httpClient) {
        this.gatewayServiceServer = gatewayServiceServer;
        this.httpClient = httpClient;
    }

    public DPATTokens fetch(
            Pair<String, String> staticCredentials, JobCredentialsMetadata jobCredentialsMetadata) {
        URL url = null;
        try {
            url = new URL(gatewayServiceServer + PATH);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Badly configured server: " + gatewayServiceServer);
        }

        ObjectMapper mapper = new ObjectMapper();
        String requestJson = buildObjectNode(jobCredentialsMetadata).toString();
        LOG.info("Request json for token exchange service {}", requestJson);
        RequestBody body = RequestBody.create(JSON, requestJson);
        String credential =
                Credentials.basic(staticCredentials.getKey(), staticCredentials.getValue());
        Request request =
                new Request.Builder()
                        .url(url)
                        .post(body)
                        .header(AUTHORIZATION_HEADER, credential)
                        .build();
        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            if (response.isSuccessful()) {
                LOG.info(
                        "Successfully exchange token for static credential {}",
                        staticCredentials.getLeft());
                return new DPATTokens(getTokenFromResponse(mapper, responseBody));
            } else {
                LOG.info(
                        "Unsuccessful response: {}, body {}, headers {}",
                        response,
                        responseBody,
                        response.headers());
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

    /**
     * BuildRequestBody will create the body according the principals provided in the
     * jobCredentialsMetadata.
     *
     * @param jobCredentialsMetadata the jobCredentialsMetadata
     * @return the jackson ObjectNode
     */
    ObjectNode buildObjectNode(JobCredentialsMetadata jobCredentialsMetadata) {

        Optional<String> serviceAccount = getServiceAccountPrincipal(jobCredentialsMetadata);
        Optional<String> user = getUserPrincipal(jobCredentialsMetadata);
        List<String> identityPools = getIdentityPoolPrincipals(jobCredentialsMetadata);

        return serviceAccount
                .map(
                        s ->
                                serviceAccountRequestBody(
                                        jobCredentialsMetadata.getStatementIdCRN(),
                                        s,
                                        jobCredentialsMetadata.getComputePoolId()))
                .orElseGet(
                        () ->
                                userRequestBody(
                                        jobCredentialsMetadata.getStatementIdCRN(),
                                        user.get(),
                                        jobCredentialsMetadata.getComputePoolId(),
                                        identityPools));
    }

    private ObjectNode serviceAccountRequestBody(
            String statementCrn, String serviceAccount, String computePoolId) {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode node = mapper.createObjectNode();
        node.put("statement_crn", statementCrn);
        node.put("compute_pool_id", computePoolId);
        node.put("service_account_id", serviceAccount);
        return node;
    }

    private ObjectNode userRequestBody(
            String statementCrn, String userId, String computePoolId, List<String> identityPools) {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode node = mapper.createObjectNode();
        node.put("statement_crn", statementCrn);
        node.put("compute_pool_id", computePoolId);
        node.put("user_resource_id", userId);

        if (!identityPools.isEmpty()) {
            ArrayNode arrayNode = node.putArray("identity_pool_ids");
            identityPools.forEach(arrayNode::add);
        }

        return node;
    }

    private Stream<String> filterByPrefix(List<String> principals, String prefix) {
        return principals.stream().filter(principal -> principal.startsWith(prefix));
    }
}
