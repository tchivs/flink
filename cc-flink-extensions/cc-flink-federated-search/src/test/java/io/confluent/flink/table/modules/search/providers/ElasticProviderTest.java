/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.providers;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.TestUtils.MockSecretDecypterProvider;
import io.confluent.flink.table.modules.ml.providers.SearchSupportedProviders;
import io.confluent.flink.table.utils.mlutils.MlUtils;
import okhttp3.Request;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for ElasticProvider. */
public class ElasticProviderTest extends ProviderTestBase {
    private final SearchSupportedProviders provider = SearchSupportedProviders.ELASTIC;

    @Test
    void testGetRequest() throws Exception {
        CatalogTable table = getCatalogTable();
        ElasticProvider elasticProvider =
                new ElasticProvider(table, new MockSecretDecypterProvider(table, metrics, clock));
        Object[] args = new Object[] {"image-vector", 10, new float[] {-5f, 9f, -12f}};
        Request request = elasticProvider.getRequest(args);
        assertThat(request.url().toString()).isEqualTo("https://fake-not-yet-validated/");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.header("Authorization")).isEqualTo("ApiKey fake-api-key");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{"
                                + "\"knn\":"
                                + "{\"field\":\"image-vector\","
                                + "\"k\":10,"
                                + "\"query_vector\":[-5.0,9.0,-12.0]}"
                                + "}");
    }

    @Test
    void testBadResponse() {
        CatalogTable table = getCatalogTable();
        ElasticProvider elasticProvider =
                new ElasticProvider(table, new MockSecretDecypterProvider(table, metrics, clock));
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                elasticProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("No '/hits/hits' field found in Elastic response.");
    }

    @Test
    void testParseResponse() {
        CatalogTable table = getCatalogTable();
        ElasticProvider elasticProvider =
                new ElasticProvider(table, new MockSecretDecypterProvider(table, metrics, clock));
        String response =
                "{\n"
                        + "    \"took\": 3,\n"
                        + "    \"timed_out\": false,\n"
                        + "    \"_shards\": {\n"
                        + "        \"total\": 1,\n"
                        + "        \"successful\": 1,\n"
                        + "        \"successful\": 1,\n"
                        + "        \"skipped\": 0,\n"
                        + "        \"failed\": 0\n"
                        + "    },\n"
                        + "    \"hits\": {\n"
                        + "        \"total\": {\n"
                        + "            \"value\": 3,\n"
                        + "            \"relation\": \"eq\"\n"
                        + "        },\n"
                        + "        \"max_score\": 0.008961825,\n"
                        + "        \"hits\": [\n"
                        + "            {\n"
                        + "                \"_index\": \"image-index\",\n"
                        + "                \"_id\": \"1\",\n"
                        + "                \"_score\": 0.008961825,\n"
                        + "                \"_source\": {\n"
                        + "                    \"image-vector\": [1, 5, -20],\n"
                        + "                    \"title-vector\": [12, 50, -10, 0, 1],\n"
                        + "                    \"title\": \"moose family\",\n"
                        + "                    \"file-type\": \"jpg\"\n"
                        + "                },\n"
                        + "                \"fields\": {\n"
                        + "                    \"title\": [\"moose family\"],\n"
                        + "                    \"file-type\": [\"jpg\"]\n"
                        + "                }\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"_index\": \"image-index\",\n"
                        + "                \"_id\": \"3\",\n"
                        + "                \"_score\": 0.0006086119,\n"
                        + "                \"_source\": {\n"
                        + "                    \"image-vector\": [15, 11, 23],\n"
                        + "                    \"title-vector\": [1, 5, 25, 50, 20],\n"
                        + "                    \"title\": \"full moon\",\n"
                        + "                    \"file-type\": \"jpg\"\n"
                        + "                },\n"
                        + "                \"fields\": {\n"
                        + "                    \"title\": [\"full moon\"],\n"
                        + "                    \"file-type\": [\"jpg\"]\n"
                        + "                }\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"_index\": \"image-index\",\n"
                        + "                \"_id\": \"2\",\n"
                        + "                \"_score\": 0.00045311023,\n"
                        + "                \"_source\": {\n"
                        + "                    \"image-vector\": [42, 8, -15],\n"
                        + "                    \"title-vector\": [25, 1, 4, -12, 2],\n"
                        + "                    \"title\": \"alpine lake\",\n"
                        + "                    \"file-type\": \"png\"\n"
                        + "                },\n"
                        + "                \"fields\": {\n"
                        + "                    \"title\": [\"alpine lake\"],\n"
                        + "                    \"file-type\": [\"png\"]\n"
                        + "                }\n"
                        + "            }\n"
                        + "        ]\n"
                        + "    }\n"
                        + "}";
        Row row = elasticProvider.getContentFromResponse(MlUtils.makeResponse(response));
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.toString())
                .isEqualTo(
                        "+I[+I[[1.0, 5.0, -20.0], [12.0, 50.0, -10.0, 0.0, 1.0], moose family, jpg],"
                                + " +I[[15.0, 11.0, 23.0], [1.0, 5.0, 25.0, 50.0, 20.0], full moon, jpg],"
                                + " +I[[42.0, 8.0, -15.0], [25.0, 1.0, 4.0, -12.0, 2.0], alpine lake, png]]");
    }

    @NotNull
    private static CatalogTable getCatalogTable() {
        Map<String, String> tableOptions = getCommonTableOptions();
        Schema inputSchema =
                Schema.newBuilder()
                        .column("image-vector", "ARRAY<FLOAT>")
                        .column("title-vector", "ARRAY<FLOAT>")
                        .column("title", "STRING")
                        .column("file-type", "STRING")
                        .build();
        return CatalogTable.of(inputSchema, "", Collections.emptyList(), tableOptions);
    }

    private static Map<String, String> getCommonTableOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("PROVIDER", "ELASTIC");
        tableOptions.put("ELASTIC.ENDPOINT", "https://fake-not-yet-validated/");
        tableOptions.put("ELASTIC.API_KEY", "fake-api-key");
        tableOptions.put("ELASTIC.FIELD", "image-vector");
        tableOptions.put("ELASTIC.K", "10");
        tableOptions.put("ELASTIC.QUERY_VECTOR", "[-5, 9, -12]");
        tableOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        return tableOptions;
    }
}
