/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.auth.oauth2.GoogleCredentials;
import io.confluent.flink.table.modules.TestUtils.IncrementingClock;
import io.confluent.flink.table.modules.TestUtils.TrackingMetricsGroup;
import io.confluent.flink.table.utils.mlutils.MlUtils;
import okhttp3.Response;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for MLModelRuntime. */
public class MLModelRuntimeTest {
    MetricGroup metricGroup;
    MLFunctionMetrics metrics;
    Clock clock = new IncrementingClock(Instant.now(), ZoneId.systemDefault());
    MockOkHttpClient mockHttpClient = new MockOkHttpClient();
    Map<String, Gauge<?>> registeredGauges = new HashMap<>();
    Map<String, Counter> registeredCounters = new HashMap<>();

    /** Mock VertexAIProvider so we can mock out the google credentials. */
    private static class MockVertexAIProvider extends VertexAIProvider {
        public MockVertexAIProvider(
                CatalogModel model, SecretDecrypterProvider secretDecrypterProvider) {
            super(model, secretDecrypterProvider);
        }

        @Override
        public GoogleCredentials getCredentials(String serviceKey) {
            return null;
        }

        @Override
        public String getAccessToken() {
            return "fake-token";
        }
    }

    @BeforeEach
    void beforeTest() {
        registeredGauges.clear();
        registeredCounters.clear();
        metricGroup = new TrackingMetricsGroup("m", registeredCounters, registeredGauges);
        metrics = new MLFunctionMetrics(metricGroup);
    }

    @Test
    void testRemoteHttpCallAzure() throws Exception {
        CatalogModel model = getAzureMLModel();
        MLModelRuntime runtime = MLModelRuntime.mockOpen(model, mockHttpClient, metrics, clock);
        mockHttpClient.withResponse(MlUtils.makeResponse("{\"output\":\"output-text\"}"));
        Row results = runtime.run(new Object[] {"modelname", "input-text"});
        runtime.close();
        Assertions.assertThat(results.toString()).isEqualTo("+I[output-text]");
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.AZUREML.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requestSuccesses").getCount())
                .isEqualTo(1);
        Assertions.assertThat(
                        registeredCounters.get("m.ConfluentML.AZUREML.requestSuccesses").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.requestMs").getValue())
                .isEqualTo(1L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.AZUREML.requestMs").getValue())
                .isEqualTo(1L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.totalMs").getValue())
                .isEqualTo(3L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.AZUREML.totalMs").getValue())
                .isEqualTo(3L);

        Assertions.assertThat(registeredCounters.get("m.ConfluentML.bytesSent").getCount())
                .isEqualTo(22);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.bytesReceived").getCount())
                .isEqualTo(24);

        Assertions.assertThat(registeredCounters.get("m.ConfluentML.request4XX").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.request5XX").getCount())
                .isEqualTo(0);
        Assertions.assertThat(
                        registeredCounters.get("m.ConfluentML.requestOtherHttpFailures").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.parseFailures").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.prepFailures").getCount())
                .isEqualTo(0);
        assertThat(registeredCounters.get("m.ConfluentML.PLAINTEXT.requestSuccesses").getCount())
                .isEqualTo(1L);
        assertThat(registeredCounters.get("m.ConfluentML.PLAINTEXT.requestFailures").getCount())
                .isEqualTo(0L);
        assertThat(registeredGauges.get("m.ConfluentML.PLAINTEXT.requestMs").getValue())
                .isEqualTo(1L);
        assertThat(
                        registeredCounters
                                .get("m.ConfluentML.AZUREML.PLAINTEXT.requestSuccesses")
                                .getCount())
                .isEqualTo(1L);
        assertThat(
                        registeredCounters
                                .get("m.ConfluentML.AZUREML.PLAINTEXT.requestFailures")
                                .getCount())
                .isEqualTo(0L);
        assertThat(registeredGauges.get("m.ConfluentML.AZUREML.PLAINTEXT.requestMs").getValue())
                .isEqualTo(1L);
    }

    @Test
    void testRemoteHttpCallAzureAI() throws Exception {
        CatalogModel model = getAzureAIModel();
        MLModelRuntime runtime = MLModelRuntime.mockOpen(model, mockHttpClient, metrics, clock);
        mockHttpClient.withResponse(makeErrorResponse("{\"error\":\"output-text\"}", 500));

        assertThatThrownBy(() -> runtime.run(new Object[] {"modelname", "input-text"}))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(
                        "Received bad response code 500 message ERROR, response: {\"error\":\"output-text\"}");
        runtime.close();
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(
                        registeredCounters.get("m.ConfluentML.AZUREML-AI.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requestSuccesses").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.request5XX").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.requestMs").getValue())
                .isEqualTo(1L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.totalMs").getValue())
                .isEqualTo(3L);
    }

    @Test
    void testRemoteHttpCallAzureOpenAI() throws Exception {
        CatalogModel model = getAzureOpenAIModel();
        MLModelRuntime runtime = MLModelRuntime.mockOpen(model, mockHttpClient, metrics, clock);
        mockHttpClient.withResponse(makeErrorResponse("{\"error\":\"output-text\"}", 400));

        assertThatThrownBy(() -> runtime.run(new Object[] {"modelname", "input-text"}))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(
                        "Received bad response code 400 message ERROR, response: {\"error\":\"output-text\"}");
        runtime.close();
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(
                        registeredCounters.get("m.ConfluentML.AZUREOPENAI.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requestSuccesses").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.request4XX").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.requestMs").getValue())
                .isEqualTo(1L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.totalMs").getValue())
                .isEqualTo(3L);
    }

    private static Response makeErrorResponse(String responseString, int code) {
        return new Response.Builder()
                .code(code)
                .message("ERROR")
                .protocol(okhttp3.Protocol.HTTP_1_1)
                .request(new okhttp3.Request.Builder().url("http://localhost").build())
                .body(okhttp3.ResponseBody.create(null, responseString))
                .build();
    }

    @Test
    void testRemoteHttpCallVertex() throws Exception {
        CatalogModel model = getVertexAIModel();
        MockVertexAIProvider vertexAIProvider =
                new MockVertexAIProvider(model, new SecretDecrypterProviderImpl(model, metrics));
        MLModelRuntime runtime =
                MLModelRuntime.mockOpen(vertexAIProvider, mockHttpClient, metrics, clock);
        mockHttpClient.withResponse(makeErrorResponse("{\"error\":\"output-text\"}", 100));

        assertThatThrownBy(() -> runtime.run(new Object[] {"modelname", "input-text"}))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(
                        "Received bad response code 100 message ERROR, response: {\"error\":\"output-text\"}");
        runtime.close();
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.provisions").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.deprovisions").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.VERTEXAI.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requestSuccesses").getCount())
                .isEqualTo(0);
        Assertions.assertThat(
                        registeredCounters.get("m.ConfluentML.requestOtherHttpFailures").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.requestMs").getValue())
                .isEqualTo(1L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.totalMs").getValue())
                .isEqualTo(3L);
    }

    @Test
    void testRemoteHttpCallVertexPub() throws Exception {
        CatalogModel model = getVertexAIPubModel();
        MockVertexAIProvider vertexAIProvider =
                new MockVertexAIProvider(model, new SecretDecrypterProviderImpl(model, metrics));
        MLModelRuntime runtime =
                MLModelRuntime.mockOpen(vertexAIProvider, mockHttpClient, metrics, clock);
        mockHttpClient.withResponse(MlUtils.makeResponse("parse-fail"));

        assertThatThrownBy(() -> runtime.run(new Object[] {"modelname", "input-text"}))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Error parsing ML Predict response");
        runtime.close();
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(
                        registeredCounters.get("m.ConfluentML.VERTEX-PUB.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requestSuccesses").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.parseFailures").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.requestMs").getValue())
                .isEqualTo(1L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.totalMs").getValue())
                .isEqualTo(3L);
    }

    class NotString {
        @Override
        public String toString() {
            throw new FlinkRuntimeException("Not a string");
        }
    }

    @Test
    void testRemoteHttpCallSagemaker() throws Exception {
        CatalogModel model = getSagemakerModel();
        MLModelRuntime runtime = MLModelRuntime.mockOpen(model, mockHttpClient, metrics, clock);
        mockHttpClient.withResponse(MlUtils.makeResponse("prep-fail"));
        Object notString = new NotString();
        assertThatThrownBy(() -> runtime.run(new Object[] {"modelname", notString}))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Not a string");
        runtime.close();
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requests").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.SAGEMAKER.requests").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requestSuccesses").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.prepFailures").getCount())
                .isEqualTo(1);
        Assertions.assertThat(
                        registeredCounters.get("m.ConfluentML.SAGEMAKER.prepFailures").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.requestMs").getValue())
                .isEqualTo(0L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentML.totalMs").getValue())
                .isEqualTo(0L);
    }

    @Test
    void testRemoteHttpCallBedrock() throws Exception {
        CatalogModel model = getBedrockModel();
        MLModelRuntime runtime = MLModelRuntime.mockOpen(model, mockHttpClient, metrics, clock);
        mockHttpClient.withResponse(MlUtils.makeResponse("{\"output\":\"output-text\"}"));
        Row results = runtime.run(new Object[] {"modelname", "input-text"});
        runtime.close();
        Assertions.assertThat(results.toString()).isEqualTo("+I[output-text]");
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.BEDROCK.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentML.requestSuccesses").getCount())
                .isEqualTo(1);
    }

    CatalogModel getAzureMLModel() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "azureml.endpoint", "https://endpoint.eastus2.inference.ml.azure.com/score");
        modelOptions.put("azureml.input_format", "JSON");
        modelOptions.put("azureml.api_key", "api-key");
        modelOptions.put("provider", "azureml");
        return getModel(modelOptions);
    }

    CatalogModel getAzureAIModel() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "azureml.endpoint",
                "https://endpoint.eastus2.inference.ai.azure.com/v1/chat/completions");
        modelOptions.put("azureml.api_key", "api-key");
        modelOptions.put("azureml.input_format", "OPENAI-CHAT");
        modelOptions.put("azureml.output_format", "json");
        modelOptions.put("provider", "azureml");
        return getModel(modelOptions);
    }

    CatalogModel getAzureOpenAIModel() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "azureopenai.endpoint",
                "https://endpoint.openai.azure.com/openai/deployments/model/chat/completions");
        modelOptions.put("azureopenai.api_key", "api-key");
        modelOptions.put("azureopenai.output_format", "json");
        modelOptions.put("provider", "azureopenai");
        return getModel(modelOptions);
    }

    CatalogModel getVertexAIModel() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "vertexai.endpoint",
                "https://us-central1-aiplatform.googleapis.com/v1/projects/1234/locations/us-central1/endpoints/1234:predict");
        modelOptions.put("vertexai.service_key", "{\"type\": \"service_account\"}");
        modelOptions.put("vertexai.output_format", "json");
        modelOptions.put("provider", "vertexai");
        return getModel(modelOptions);
    }

    CatalogModel getVertexAIPubModel() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "vertexai.endpoint",
                "https://us-central1-aiplatform.googleapis.com/v1/projects/1234/locations/us-central1/publishers/google/1234:predict");
        modelOptions.put("vertexai.service_key", "{\"type\": \"service_account\"}");
        modelOptions.put("vertexai.output_format", "json");
        modelOptions.put("provider", "vertexai");
        return getModel(modelOptions);
    }

    CatalogModel getSagemakerModel() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "sagemaker.endpoint",
                "https://runtime.sagemaker.us-west-2.amazonaws.com/endpoints/jumpstart-dft-sentence-encoder-cmlm-20240419-230337/invocations");
        modelOptions.put("sagemaker.input_format", "JSONARRAY");
        modelOptions.put("sagemaker.input_content_type", "application/x-text");
        modelOptions.put("sagemaker.output_format", "json");
        modelOptions.put("sagemaker.INFERENCE_COMPONENT_NAME", "ABCD1234");
        modelOptions.put("sagemaker.AWS_ACCESS_KEY_ID", "ABCD");
        modelOptions.put("sagemaker.AWS_SECRET_ACCESS_KEY", "1234");
        modelOptions.put("provider", "sagemaker");
        return getModel(modelOptions);
    }

    CatalogModel getBedrockModel() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "bedrock.endpoint",
                "https://bedrock-runtime.us-west-2.amazonaws.com/model/amazon.titan-text-lite-v1/invoke");
        modelOptions.put("bedrock.AWS_ACCESS_KEY_ID", "ABCD");
        modelOptions.put("bedrock.AWS_SECRET_ACCESS_KEY", "1234");
        modelOptions.put("bedrock.output_format", "json");
        modelOptions.put("provider", "bedrock");
        return getModel(modelOptions);
    }

    CatalogModel getModel(Map<String, String> modelOptions) {
        // inputs
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        ResolvedSchema resolvedInputSchema =
                ResolvedSchema.of(Column.physical("input", DataTypes.STRING()));
        // outputs
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        ResolvedSchema resolvedOutputSchema =
                ResolvedSchema.of(Column.physical("output", DataTypes.STRING()));
        CatalogModel catalogModel = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        ResolvedCatalogModel model =
                ResolvedCatalogModel.of(catalogModel, resolvedInputSchema, resolvedOutputSchema);
        return catalogModel;
    }
}
