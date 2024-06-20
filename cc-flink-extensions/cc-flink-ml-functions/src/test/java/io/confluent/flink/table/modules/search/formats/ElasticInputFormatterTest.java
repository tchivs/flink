/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.ml.formats.TextGenerationParams;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for elastic input formatter. */
public class ElasticInputFormatterTest {
    @Test
    void testGetRequest() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("field", "STRING")
                        .column("k", "BIGINT")
                        .column("query_vector", "ARRAY<FLOAT>")
                        .build();

        ElasticInputFormatter formatter =
                new ElasticInputFormatter(inputSchema.getColumns(), getBaseParams());
        Object[] args = new Object[] {"image-vector", 3L, new float[] {0.1f, 0.2f, 0.3f, 0.4f}};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"field\":\"image-vector\",\"k\":3,\"query_vector\":[0.1,0.2,0.3,0.4]}");
    }

    @Test
    void testGetRequestWithFilter() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("field", "STRING")
                        .column("k", "BIGINT")
                        .column("query_vector", "ARRAY<DOUBLE>")
                        .build();
        ElasticInputFormatter formatter =
                new ElasticInputFormatter(inputSchema.getColumns(), getFilterParams());
        Object[] args = new Object[] {"image-vector", 3L, new double[] {0.1, 0.2, 0.3, 0.4}};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"filter\":{\"term\":{\"file-type\":\"png\"}},\"similarity\":0.3,\"field\":\"image-vector\",\"k\":3,\"query_vector\":[0.1,0.2,0.3,0.4]}");
    }

    @Test
    void testGetRequestInvalidEmbedding() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("field", "STRING")
                        .column("k", "BIGINT")
                        .column("query_vector", "DOUBLE")
                        .build();
        assertThatThrownBy(
                        () -> new ElasticInputFormatter(inputSchema.getColumns(), getBaseParams()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Elastic query_vector input must be an array, got DOUBLE");
    }

    @Test
    void testGetRequestInvalidTopK() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("field", "STRING")
                        .column("k", "STRING")
                        .column("query_vector", "ARRAY<FLOAT>")
                        .build();
        assertThatThrownBy(
                        () -> new ElasticInputFormatter(inputSchema.getColumns(), getBaseParams()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Elastic k input must be an integer, got STRING");
    }

    @Test
    void testGetRequestInvalidField() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("field", "DOUBLE")
                        .column("k", "BIGINT")
                        .column("query_vector", "ARRAY<FLOAT>")
                        .build();
        assertThatThrownBy(
                        () -> new ElasticInputFormatter(inputSchema.getColumns(), getBaseParams()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Elastic field input must be a string, got DOUBLE");
    }

    private static TextGenerationParams getBaseParams() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("PROVIDER", "Elastic");
        modelOptions.put("ELASTIC.field", "image-vector");
        return new TextGenerationParams(modelOptions);
    }

    private static TextGenerationParams getFilterParams() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("PROVIDER", "ELASTIC");
        modelOptions.put("ELASTIC.PARAMS.filter.term.file-type", "png");
        modelOptions.put("ELASTIC.field", "image-vector");
        modelOptions.put("ELASTIC.PARAMS.similarity", "0.3");
        return new TextGenerationParams(modelOptions);
    }
}
