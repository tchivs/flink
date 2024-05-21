/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.util.Preconditions;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okio.Timeout;

import java.io.IOException;

/** Mocks {@link OkHttpClient}. */
@Confluent
public class MockOkHttpClient extends OkHttpClient {

    private Response response;
    private boolean errorOnCall;

    public MockOkHttpClient withResponse(Response response) {
        this.response = response;
        return this;
    }

    public MockOkHttpClient withErrorOnCall() {
        errorOnCall = true;
        return this;
    }

    @Override
    public Call newCall(Request request) {
        Preconditions.checkState(response != null || errorOnCall);
        return new MockCall(request, response, errorOnCall);
    }

    /** A mock Call object. */
    public static class MockCall implements Call {

        private final Request request;
        private final Response response;
        private final boolean errorOnCall;

        public MockCall(Request request, Response response, boolean errorOnCall) {

            this.request = request;
            this.response = response;
            this.errorOnCall = errorOnCall;
        }

        @Override
        public Request request() {
            return request;
        }

        @Override
        public Response execute() throws IOException {
            if (errorOnCall) {
                throw new RuntimeException("Error!");
            }
            return response;
        }

        @Override
        public void enqueue(Callback callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancel() {}

        @Override
        public boolean isExecuted() {
            return false;
        }

        @Override
        public boolean isCanceled() {
            return false;
        }

        @Override
        public Timeout timeout() {
            return null;
        }

        @Override
        public Call clone() {
            return null;
        }
    }
}
