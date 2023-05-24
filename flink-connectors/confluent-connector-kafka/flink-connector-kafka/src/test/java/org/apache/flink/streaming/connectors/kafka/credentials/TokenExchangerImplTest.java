/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.connectors.kafka.credentials.utils.MockOkHttpClient;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.streaming.connectors.kafka.credentials.TokenExchangerImpl.getTokenFromResponse;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link TokenExchangerImpl}. */
@Confluent
public class TokenExchangerImplTest {

    private static final JobCredentialsMetadata METADATA =
            new JobCredentialsMetadata(JobID.generate(), "crn://", "computepool", "identity", 0, 0);

    private TokenExchangerImpl exchanger;
    private MockOkHttpClient httpClientMock;

    @Before
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
    public void test_fetch() throws Exception {
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
    public void test_fetch_internalError() throws Exception {
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
    public void test_fetch_clientError() throws Exception {
        httpClientMock.withErrorOnCall();
        assertThatThrownBy(() -> exchanger.fetch(Pair.of("key", "secret"), METADATA))
                .hasMessageContaining("Error!");
    }
}
