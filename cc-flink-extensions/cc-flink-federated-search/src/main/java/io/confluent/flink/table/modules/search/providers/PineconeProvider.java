/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.providers;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.types.Row;

import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.modules.ml.formats.TextGenerationParams;
import io.confluent.flink.table.modules.ml.providers.SearchSupportedProviders;
import io.confluent.flink.table.modules.ml.secrets.SecretDecrypterProvider;
import io.confluent.flink.table.modules.search.formats.PineconeInputFormatter;
import io.confluent.flink.table.modules.search.formats.PineconeOutputParser;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.List;
import java.util.Map;

/** Provider for Pinecone search. */
public class PineconeProvider implements SearchRuntimeProvider {
    private static final String API_KEY_HEADER = "Api-Key";
    private final SecretDecrypterProvider secretDecrypterProvider;
    private final String apiKey;
    private final String endpoint;
    private final InputFormatter inputFormatter;
    private final MediaType contentType;
    private final OutputParser outputParser;

    public PineconeProvider(CatalogTable table, SecretDecrypterProvider secretDecrypterProvider) {
        this.secretDecrypterProvider = secretDecrypterProvider;
        SearchSupportedProviders supportedProvider = SearchSupportedProviders.PINECONE;
        String namespace = supportedProvider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(table.getOptions());
        this.endpoint = modelOptionsUtils.getProviderOption(MLModelCommonConstants.ENDPOINT);
        // TODO: Validate endpoint
        // TODO: Move the PINECONE.API_KEY constant somewhere. Options class like ML model options?
        this.apiKey =
                secretDecrypterProvider
                        .getMeteredDecrypter(modelOptionsUtils.getEncryptStrategy())
                        .decryptFromKey("PINECONE.API_KEY");
        // TODO: Pass the topK/vector input schema through from somewhere. The function context?.
        List<Schema.UnresolvedColumn> inputColumns = new java.util.ArrayList<>();
        inputColumns.add(new Schema.UnresolvedPhysicalColumn("topK", DataTypes.INT()));
        inputColumns.add(
                new Schema.UnresolvedPhysicalColumn("vector", DataTypes.ARRAY(DataTypes.FLOAT())));
        // TODO: Pass through the name of the embedding column from somewhere. The table options?
        String embeddingColumnName = "embedding";
        this.inputFormatter =
                new PineconeInputFormatter(
                        inputColumns, new TextGenerationParams(table.getOptions()));
        contentType = MediaType.parse(inputFormatter.contentType());
        this.outputParser =
                new PineconeOutputParser(
                        table.getUnresolvedSchema().getColumns(), embeddingColumnName);
    }

    @Override
    public RequestBody getRequestBody(Object[] args) {
        return RequestBody.create(contentType, inputFormatter.format(args));
    }

    @Override
    public Request getRequest(Object[] args) {
        Request.Builder builder =
                new Request.Builder()
                        .url(endpoint)
                        .post(getRequestBody(args))
                        .header(API_KEY_HEADER, apiKey);
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
        return SearchSupportedProviders.PINECONE.getProviderName();
    }
}
