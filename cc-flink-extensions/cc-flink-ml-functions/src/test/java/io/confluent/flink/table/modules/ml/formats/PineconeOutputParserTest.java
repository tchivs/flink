/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for pinecone output parser. */
public class PineconeOutputParserTest {
    @Test
    void testParse() {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("content", "STRING")
                        .column("content2", "INT")
                        .column("embeds", "ARRAY<DOUBLE>")
                        .build();
        OutputParser parser = new PineconeOutputParser(inputSchema.getColumns(), "embeds");
        String response =
                "{\n"
                        + "  \"matches\":[\n"
                        + "    {\n"
                        + "      \"id\": \"vec3\",\n"
                        + "      \"score\": 0,\n"
                        + "      \"values\": [0.3,0.3,0.3,0.3],\n"
                        + "      \"metadata\": {\"key\": \"value1\", \"content\": \"content1\", \"content2\": 3}\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"id\": \"vec2\",\n"
                        + "      \"score\": 0.0800000429,\n"
                        + "      \"values\": [0.2, 0.2, 0.2, 0.2],\n"
                        + "      \"metadata\": {\"key\": \"value2\", \"content\": \"content1\", \"content2\": 4}\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"id\": \"vec4\",\n"
                        + "      \"score\": 0.0799999237,\n"
                        + "      \"values\": [0.4, 0.4, 0.4, 0.4],\n"
                        + "      \"metadata\": {\"key\": \"value3\", \"content\": \"content1\", \"content2\": 5}\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"namespace\": \"example-namespace\",\n"
                        + "  \"usage\": {\"read_units\": 6}\n"
                        + "}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo(
                        "+I[+I[content1, 3, [0.3, 0.3, 0.3, 0.3]], +I[content1, 4, [0.2, 0.2, 0.2, 0.2]], +I[content1, 5, [0.4, 0.4, 0.4, 0.4]]]");
    }

    @Test
    void testParseMissingOutput() {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("content", "STRING")
                        .column("content2", "INT")
                        .column("missing", "STRING")
                        .column("embeds", "ARRAY<DOUBLE>")
                        .build();
        OutputParser parser = new PineconeOutputParser(inputSchema.getColumns(), "embeds");
        String response =
                "{\n"
                        + "  \"matches\":[\n"
                        + "    {\n"
                        + "      \"id\": \"vec3\",\n"
                        + "      \"score\": 0,\n"
                        + "      \"values\": [0.3,0.3,0.3,0.3],\n"
                        + "      \"metadata\": {\"key\": \"value1\", \"content\": \"content1\", \"content2\": 3}\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"id\": \"vec2\",\n"
                        + "      \"score\": 0.0800000429,\n"
                        + "      \"values\": [0.2, 0.2, 0.2, 0.2],\n"
                        + "      \"metadata\": {\"key\": \"value2\", \"content\": \"content1\", \"content2\": 4}\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"id\": \"vec4\",\n"
                        + "      \"score\": 0.0799999237,\n"
                        + "      \"values\": [0.4, 0.4, 0.4, 0.4],\n"
                        + "      \"metadata\": {\"key\": \"value3\", \"content\": \"content1\", \"content2\": 5}\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"namespace\": \"example-namespace\",\n"
                        + "  \"usage\": {\"read_units\": 6}\n"
                        + "}";
        assertThatThrownBy(() -> parser.parse(MlUtils.makeResponse(response)).toString())
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Pinecone 'metadata' field did not contain a field named 'missing' for match index 0");
    }

    @Test
    void testParseMissingEmbedding() {
        Schema inputSchema =
                Schema.newBuilder().column("content", "STRING").column("content2", "INT").build();
        assertThatThrownBy(() -> new PineconeOutputParser(inputSchema.getColumns(), "embeds"))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Pinecone output columns did not contain the specified embedding column named 'embeds'");
    }
}
