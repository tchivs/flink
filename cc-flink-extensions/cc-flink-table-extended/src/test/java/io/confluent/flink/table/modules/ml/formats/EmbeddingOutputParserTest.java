/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;

import io.confluent.flink.table.utils.mlutils.MlUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for embedding output parser. */
public class EmbeddingOutputParserTest {
    @Test
    void testParseAmazonTitanEmbed() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<ARRAY<FLOAT>>").build();
        OutputParser parser =
                new EmbeddingOutputParser(outputSchema.getColumns(), "Amazon Titan Embed");
        String response = "{\"embedding\":[[1.0, 2.0], [3.0, 4.0]]}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[[[1.0, 2.0], [3.0, 4.0]]]");
    }

    @Test
    void testParseCohereEmbed() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<FLOAT>").build();
        OutputParser parser = new EmbeddingOutputParser(outputSchema.getColumns(), "Cohere Embed");
        String response = "{\"embeddings\":[1.0, 2.0, 3.0, 4.0]}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[[1.0, 2.0, 3.0, 4.0]]");
    }

    @Test
    void testParseOpenAIEmbed() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<FLOAT>").build();
        OutputParser parser = new EmbeddingOutputParser(outputSchema.getColumns(), "OpenAI Embed");
        String response = "{\"data\":[{\"embedding\":[1.0, 2.0, 3.0, 4.0]}]}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[[1.0, 2.0, 3.0, 4.0]]");
    }

    @Test
    void testParseVertexEmbed() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<ARRAY<FLOAT>>").build();
        OutputParser parser = new EmbeddingOutputParser(outputSchema.getColumns(), "Vertex Embed");
        String response =
                "{\"predictions\":[{\"embeddings\":{\"values\":[[1.0, 2.0], [3.0, 4.0]]}}]}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[[[1.0, 2.0], [3.0, 4.0]]]");
    }
}
