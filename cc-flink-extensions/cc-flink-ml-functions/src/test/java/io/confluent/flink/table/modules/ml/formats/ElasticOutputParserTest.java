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

/** Unit tests for elastic output parser. */
public class ElasticOutputParserTest {
    @Test
    void testParse() {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("content", "STRING")
                        .column("content2", "INT")
                        .column("embeds", "ARRAY<DOUBLE>")
                        .build();
        OutputParser parser = new ElasticOutputParser(inputSchema.getColumns(), "embeds");
        String response =
                "{\n"
                        + "  \"took\": 5,\n"
                        + "  \"timed_out\": false,\n"
                        + "  \"_shards\": {\n"
                        + "    \"total\": 1,\n"
                        + "    \"successful\": 1,\n"
                        + "    \"skipped\": 0,\n"
                        + "    \"failed\": 0\n"
                        + "  },\n"
                        + "  \"hits\": {\n"
                        + "    \"total\": {\n"
                        + "      \"value\": 20,\n"
                        + "      \"relation\": \"eq\"\n"
                        + "    },\n"
                        + "    \"max_score\": 1.3862942,\n"
                        + "    \"hits\": [\n"
                        + "      {\n"
                        + "        \"_index\": \"my-index-000001\",\n"
                        + "        \"_id\": \"3\",\n"
                        + "        \"_score\": 1.3862942,\n"
                        + "        \"_source\": {\n"
                        + "          \"content\": \"content1\",\n"
                        + "          \"content2\": 3,\n"
                        + "          \"@timestamp\": \"2099-11-15T14:12:12\"\n"
                        + "        }\n"
                        + "      },\n"
                        + "      {\n"
                        + "        \"_index\": \"my-index-000001\",\n"
                        + "        \"_id\": \"5\",\n"
                        + "        \"_score\": 1.4862942,\n"
                        + "        \"_source\": {\n"
                        + "          \"content\": \"content2\",\n"
                        + "          \"content2\": 4,\n"
                        + "          \"@timestamp\": \"2001-01-01T14:00:00\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }\n"
                        + "}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[+I[content1, 3, null], +I[content2, 4, null]]");
    }

    @Test
    void testParseNoEmbedding() {
        Schema inputSchema =
                Schema.newBuilder().column("content", "STRING").column("content2", "INT").build();
        OutputParser parser = new ElasticOutputParser(inputSchema.getColumns(), "embeds");
        String response =
                "{\n"
                        + "  \"hits\": {\n"
                        + "    \"hits\": [\n"
                        + "      {\n"
                        + "        \"_index\": \"my-index-000001\",\n"
                        + "        \"_id\": \"3\",\n"
                        + "        \"_score\": 1.3862942,\n"
                        + "        \"_source\": {\n"
                        + "          \"content\": \"content1\",\n"
                        + "          \"content2\": 3\n"
                        + "        }\n"
                        + "      },\n"
                        + "      {\n"
                        + "        \"_index\": \"my-index-000001\",\n"
                        + "        \"_id\": \"5\",\n"
                        + "        \"_score\": 1.4862942,\n"
                        + "        \"_source\": {\n"
                        + "          \"content\": \"content2\",\n"
                        + "          \"content2\": 4\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }\n"
                        + "}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[+I[content1, 3], +I[content2, 4]]");
    }

    @Test
    void testParseMissingColumn() {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("content", "STRING")
                        .column("content2", "INT")
                        .column("missing", "INT")
                        .build();
        OutputParser parser = new ElasticOutputParser(inputSchema.getColumns(), "embeds");
        String response =
                "{\n"
                        + "  \"hits\": {\n"
                        + "    \"hits\": [\n"
                        + "      {\n"
                        + "        \"_index\": \"my-index-000001\",\n"
                        + "        \"_id\": \"3\",\n"
                        + "        \"_score\": 1.3862942,\n"
                        + "        \"_source\": {\n"
                        + "          \"content\": \"content1\",\n"
                        + "          \"content2\": 3\n"
                        + "        }\n"
                        + "      },\n"
                        + "      {\n"
                        + "        \"_index\": \"my-index-000001\",\n"
                        + "        \"_id\": \"5\",\n"
                        + "        \"_score\": 1.4862942,\n"
                        + "        \"_source\": {\n"
                        + "          \"content\": \"content2\",\n"
                        + "          \"content2\": 4\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }\n"
                        + "}";
        assertThatThrownBy(() -> parser.parse(MlUtils.makeResponse(response)).toString())
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Elastic '_source' field did not contain a field named 'missing' for hit index 0");
    }
}
