/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.credentials.utils.MockOkHttpClient;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static io.confluent.flink.credentials.TokenExchangerImpl.getTokenFromResponse;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link TokenExchangerImpl}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class TokenExchangerImplTest {

    private static final JobCredentialsMetadata METADATA =
            new JobCredentialsMetadata(
                    JobID.generate(),
                    "crn://",
                    "computepool",
                    "identity",
                    Collections.emptyList(),
                    0,
                    0);

    private static final JobCredentialsMetadata SA_PRINCIPAL_METADATA =
            new JobCredentialsMetadata(
                    JobID.generate(),
                    "crn://",
                    "computepool",
                    "",
                    Collections.singletonList("sa-123"),
                    0,
                    0);

    private static final JobCredentialsMetadata USER_PRINCIPAL_METADATA =
            new JobCredentialsMetadata(
                    JobID.generate(),
                    "crn://",
                    "computepool",
                    "",
                    Collections.singletonList("u-123"),
                    0,
                    0);

    private static final JobCredentialsMetadata USER_POOL_PRINCIPAL_METADATA =
            new JobCredentialsMetadata(
                    JobID.generate(),
                    "crn://",
                    "computepool",
                    "",
                    Arrays.stream(new String[] {"u-123", "pool-123", "pool-234"})
                            .collect(Collectors.toList()),
                    0,
                    0);

    private TokenExchangerImpl exchanger;
    private MockOkHttpClient httpClientMock;

    @BeforeEach
    public void setUp() {
        httpClientMock = new MockOkHttpClient();
        exchanger = new TokenExchangerImpl("http://server", httpClientMock);
    }

    @Test
    public void test_parseResponse() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        assertThat(getTokenFromResponse(objectMapper, "{\"token\":\"abc\", \"error\":null}"))
                .isEqualTo("abc");
        assertThatThrownBy(
                        () ->
                                getTokenFromResponse(
                                        objectMapper, "{\"token\":null, \"error\":null}"))
                .hasMessage("Error fetching token: <no message>");
        assertThatThrownBy(
                        () ->
                                getTokenFromResponse(
                                        objectMapper, "{\"token\":null, \"error\":\"Error!!!\"}"))
                .hasMessage("Error fetching token: Error!!!");
        assertThatThrownBy(
                        () ->
                                getTokenFromResponse(
                                        objectMapper, "{\"token\":{}, \"error\":\"Error!!!\"}"))
                .hasMessage("Error fetching token: Error!!!");
        assertThatThrownBy(() -> getTokenFromResponse(objectMapper, "{\"token\":{}, \"error\":{}}"))
                .hasMessage("Error fetching token: <no message>");
    }

    @Test
    public void test_fetch() {
        Request request = new Request.Builder().url("http://example.com").build();
        Response response =
                new Response.Builder()
                        .request(request)
                        .protocol(Protocol.HTTP_1_1)
                        .code(200)
                        .message("Success")
                        .body(
                                ResponseBody.create(
                                        MediaType.get("application/json"),
                                        "{\"token\":\"abc\", \"error\":null}"))
                        .build();
        httpClientMock.withResponse(response);
        DPATToken token = exchanger.fetch(Pair.of("key", "secret"), METADATA);
        assertThat(token.getToken()).isEqualTo("abc");
    }

    @Test
    public void test_fetch_service_account_principal() {
        Request request = new Request.Builder().url("http://example.com").build();
        Response response =
                new Response.Builder()
                        .request(request)
                        .protocol(Protocol.HTTP_1_1)
                        .code(200)
                        .message("Success")
                        .body(
                                ResponseBody.create(
                                        MediaType.get("application/json"),
                                        "{\"token\":\"abc\", \"error\":null}"))
                        .build();
        httpClientMock.withResponse(response);
        DPATToken token = exchanger.fetch(Pair.of("key", "secret"), SA_PRINCIPAL_METADATA);
        assertThat(token.getToken()).isEqualTo("abc");
    }

    @Test
    public void test_fetch_user_principal() {
        Request request = new Request.Builder().url("http://example.com").build();
        Response response =
                new Response.Builder()
                        .request(request)
                        .protocol(Protocol.HTTP_1_1)
                        .code(200)
                        .message("Success")
                        .body(
                                ResponseBody.create(
                                        MediaType.get("application/json"),
                                        "{\"token\":\"abc\", \"error\":null}"))
                        .build();
        httpClientMock.withResponse(response);
        DPATToken token = exchanger.fetch(Pair.of("key", "secret"), USER_PRINCIPAL_METADATA);
        assertThat(token.getToken()).isEqualTo("abc");
    }

    @Test
    public void test_fetch_user_pools_principal() {
        Request request = new Request.Builder().url("http://example.com").build();
        Response response =
                new Response.Builder()
                        .request(request)
                        .protocol(Protocol.HTTP_1_1)
                        .code(200)
                        .message("Success")
                        .body(
                                ResponseBody.create(
                                        MediaType.get("application/json"),
                                        "{\"token\":\"abc\", \"error\":null}"))
                        .build();
        httpClientMock.withResponse(response);
        DPATToken token = exchanger.fetch(Pair.of("key", "secret"), USER_POOL_PRINCIPAL_METADATA);
        assertThat(token.getToken()).isEqualTo("abc");
    }

    @Test
    public void test_build_request_body_sa() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode saNode = exchanger.buildObjectNode(SA_PRINCIPAL_METADATA);

        String json =
                "{\"statement_crn\":\"crn://\",\"compute_pool_id\":\"computepool\",\"service_account_id\":\"sa-123\"}";
        ObjectNode actualNode = mapper.readValue(json, ObjectNode.class);
        assertThat(saNode).isEqualTo(actualNode);
    }

    @Test
    public void test_build_request_body_user() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode saNode = exchanger.buildObjectNode(USER_PRINCIPAL_METADATA);

        String json =
                "{\"statement_crn\":\"crn://\",\"compute_pool_id\":\"computepool\",\"user_id\":\"u-123\"}";
        ObjectNode actualNode = mapper.readValue(json, ObjectNode.class);
        assertThat(saNode).isEqualTo(actualNode);
    }

    @Test
    public void test_build_request_body_user_pools() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode saNode = exchanger.buildObjectNode(USER_POOL_PRINCIPAL_METADATA);

        String json =
                "{\"statement_crn\":\"crn://\",\"compute_pool_id\":\"computepool\",\"user_id\":\"u-123\",\"identity_pool_ids\":[\"pool-123\",\"pool-234\"]}";
        ObjectNode actualNode = mapper.readValue(json, ObjectNode.class);
        assertThat(saNode).isEqualTo(actualNode);
    }

    @Test
    public void test_build_request_body_identity_pool_precedence() throws JsonProcessingException {

        JobCredentialsMetadata identityAndPrincipalsMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(),
                        "crn://",
                        "computepool",
                        "identityPool",
                        Collections.singletonList("sa-123"),
                        0,
                        0);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode saNode = exchanger.buildObjectNode(identityAndPrincipalsMetadata);

        String json = "{\"statement_id\":\"crn://\",\"compute_pool_id\":\"computepool\"}";
        ObjectNode actualNode = mapper.readValue(json, ObjectNode.class);
        assertThat(saNode).isEqualTo(actualNode);
    }

    @Test
    public void test_fetch_internalError() {
        Request request = new Request.Builder().url("http://example.com").build();
        Response response =
                new Response.Builder()
                        .request(request)
                        .protocol(Protocol.HTTP_1_1)
                        .code(500)
                        .message("Failure")
                        .body(
                                ResponseBody.create(
                                        MediaType.get("application/json"),
                                        "{\"token\":null, \"error\":null}"))
                        .build();
        httpClientMock.withResponse(response);
        assertThatThrownBy(() -> exchanger.fetch(Pair.of("key", "secret"), METADATA))
                .hasMessageContaining("Received bad response code 500");
    }

    @Test
    public void test_fetch_clientError() {
        httpClientMock.withErrorOnCall();
        assertThatThrownBy(() -> exchanger.fetch(Pair.of("key", "secret"), METADATA))
                .hasMessageContaining("Error!");
    }
}
