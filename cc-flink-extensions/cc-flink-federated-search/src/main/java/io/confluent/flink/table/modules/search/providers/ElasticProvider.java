/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.providers;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.types.Row;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.modules.ml.formats.TextGenerationParams;
import io.confluent.flink.table.modules.ml.providers.SearchSupportedProviders;
import io.confluent.flink.table.modules.ml.secrets.SecretDecrypterProvider;
import io.confluent.flink.table.modules.search.formats.ElasticInputFormatter;
import io.confluent.flink.table.modules.search.formats.ElasticOutputParser;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.List;
import java.util.Map;

/** Provider for ElasticSearch search. */
public class ElasticProvider implements SearchRuntimeProvider {
    private static final String API_KEY_HEADER = "ApiKey";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String KNN_HEADER = "knn";
    private final SecretDecrypterProvider secretDecrypterProvider;
    // Do we require apiKey? On-prem elastic can work without apiKey
    private final String apiKey;
    // Do we make index as part of the endpoint?
    private final String endpoint;
    private final InputFormatter inputFormatter;
    private final MediaType contentType;
    private final OutputParser outputParser;
    private ObjectMapper mapper = new ObjectMapper();

    public ElasticProvider(CatalogTable table, SecretDecrypterProvider secretDecrypterProvider) {
        this.secretDecrypterProvider = secretDecrypterProvider;
        SearchSupportedProviders supportedProvider = SearchSupportedProviders.ELASTIC;
        String namespace = supportedProvider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(table.getOptions());
        this.endpoint = modelOptionsUtils.getProviderOption(MLModelCommonConstants.ENDPOINT);
        this.apiKey =
                secretDecrypterProvider
                        .getMeteredDecrypter(modelOptionsUtils.getEncryptStrategy())
                        .decryptFromKey("ELASTIC.API_KEY");
        List<Schema.UnresolvedColumn> inputColumns = new java.util.ArrayList<>();
        inputColumns.add(new Schema.UnresolvedPhysicalColumn("field", DataTypes.STRING()));
        inputColumns.add(new Schema.UnresolvedPhysicalColumn("k", DataTypes.INT()));
        inputColumns.add(
                new Schema.UnresolvedPhysicalColumn(
                        "query_vector", DataTypes.ARRAY(DataTypes.FLOAT())));
        String embeddingColumnName = "embedding";
        this.inputFormatter =
                new ElasticInputFormatter(
                        inputColumns, new TextGenerationParams(table.getOptions()));
        contentType = MediaType.parse(inputFormatter.contentType());
        this.outputParser =
                new ElasticOutputParser(
                        table.getUnresolvedSchema().getColumns(), embeddingColumnName);
    }

    @Override
    /** format request body with knn. */
    public RequestBody getRequestBody(Object[] args) {
        byte[] argsJson = inputFormatter.format(args);
        try {
            JsonNode argsNode = mapper.readTree(argsJson);
            ObjectNode rootNode = mapper.createObjectNode();
            rootNode.set(KNN_HEADER, argsNode);
            byte[] requestBody = mapper.writeValueAsBytes(rootNode);
            return RequestBody.create(contentType, requestBody);
        } catch (Exception e) {
            throw new RuntimeException("Error processing Elastic request: ", e);
        }
    }

    @Override
    public Request getRequest(Object[] args) {
        Request.Builder builder =
                new Request.Builder()
                        .url(endpoint)
                        .post(getRequestBody(args))
                        .header(AUTHORIZATION_HEADER, API_KEY_HEADER + " " + apiKey);
        for (Map.Entry<String, String> header : inputFormatter.headers()) {
            builder.header(header.getKey(), header.getValue());
        }
        return builder.build();
    }

    @Override
    public Row getContentFromResponse(Response response) {
        return outputParser.parse(response);
    }

    @Override
    public String maskSecrets(String message) {
        return message.replaceAll(apiKey, "*****");
    }

    @Override
    public String getMetricsName() {
        return SearchSupportedProviders.ELASTIC.getProviderName();
    }
}
