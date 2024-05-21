/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unittests for TextGenerationParams. */
public class TextGenerationParamsTest {
    private final transient ObjectMapper mapper = new ObjectMapper();

    @Test
    void testLinkModelVersion() {
        TextGenerationParams params = getTextGenerationParams();
        ObjectNode node = mapper.createObjectNode();

        params.linkModelVersion(node, "model", "gpt-3.5-turbo");
        assertThat(node.get("model").asText()).isEqualTo("model-123");
    }

    @Test
    void testLinkTemperature() {
        ObjectNode node = mapper.createObjectNode();
        TextGenerationParams params = getTextGenerationParams();

        String usedParam = params.linkTemperature(node, "temperature");
        assertThat(usedParam).isEqualTo("temperature");
        assertThat(node.get("temperature").asDouble()).isEqualTo(0.5);
    }

    @Test
    void testLinkTopP() {
        ObjectNode node = mapper.createObjectNode();
        TextGenerationParams params = getTextGenerationParams();

        String usedParam = params.linkTopP(node, "top_p");
        assertThat(usedParam).isEqualTo("p");
        assertThat(node.get("top_p").asDouble()).isEqualTo(0.9);
    }

    @Test
    void testLinkMaxTokens() {
        ObjectNode node = mapper.createObjectNode();
        TextGenerationParams params = getTextGenerationParams();

        String usedParam = params.linkMaxTokens(node, "max_tokens");
        assertThat(usedParam).isEqualTo("maxtokenstosample");
        assertThat(node.get("max_tokens").asInt()).isEqualTo(100);
    }

    @Test
    void testLinkStopSequences() {
        ObjectNode node = mapper.createObjectNode();
        TextGenerationParams params = getTextGenerationParams();

        String usedParam = params.linkStopSequences(node, "stop");
        assertThat(usedParam).isEqualTo("stop_sequences");
        ArrayNode stopSequences = (ArrayNode) node.get("stop");
        assertThat(stopSequences.get(0).asText()).isEqualTo("stop1");
        assertThat(stopSequences.get(1).asText()).isEqualTo("stop2");
    }

    @Test
    void testLinkSystemPrompt() {
        ObjectNode node = mapper.createObjectNode();
        TextGenerationParams params = getTextGenerationParams();

        params.linkSystemPrompt(node, "content");
        assertThat(node.get("content").asText()).isEqualTo("System Prompt!");
    }

    @Test
    void testLinkOtherParams() {
        ObjectNode node = mapper.createObjectNode();
        TextGenerationParams params = getTextGenerationParams();

        String usedParam = params.linkParam(node, "extra", "these", "don't", "exist");
        assertThat(usedParam).isEqualTo(null);
        // The field shouldn't have been created.
        assertThat(node.has("extra")).isFalse();

        usedParam = params.linkParam(node, "extra", "foo", "extra", "extra2");
        assertThat(usedParam).isEqualTo("extra");
        assertThat(node.get("extra").isLong()).isTrue();
        assertThat(node.get("extra").asInt()).isEqualTo(1);

        params.linkParam(node, "extra2", "extra2");
        assertThat(node.get("extra2").isArray()).isTrue();
        ArrayNode extraSequences = (ArrayNode) node.get("extra2");
        assertThat(extraSequences.size()).isEqualTo(3);
        assertThat(extraSequences.get(0).asText()).isEqualTo("extra");
        assertThat(extraSequences.get(1).asText()).isEqualTo("extra");
        assertThat(extraSequences.get(2).asText()).isEqualTo("extra");

        params.linkParam(node, "extra3", "extra3");
        assertThat(node.get("extra3").isLong()).isTrue();
        assertThat(node.get("extra3").asInt()).isEqualTo(123);

        params.linkParam(node, "extra4", "extra4");
        assertThat(node.get("extra4").isDouble()).isTrue();
        assertThat(node.get("extra4").asDouble()).isEqualTo(123.0);

        params.linkParam(node, "extra5", "extra5");
        assertThat(node.get("extra5").isBoolean()).isTrue();
        assertThat(node.get("extra5").asBoolean()).isTrue();

        params.linkParam(node, "extra6", "extra6");
        assertThat(node.get("extra6").isBoolean()).isFalse();
        assertThat(node.get("extra6").isTextual()).isTrue();
        assertThat(node.get("extra6").asText()).isEqualTo("true");

        params.linkParam(node, "duplicate1", "duplicate1");
        assertThat(node.get("duplicate1").isLong()).isTrue();
        assertThat(node.get("duplicate1").asInt()).isEqualTo(123);
    }

    @Test
    void testLinkAllParams() {
        ObjectNode node = mapper.createObjectNode();
        TextGenerationParams params = getTextGenerationParams();
        Set<String> usedParams = new HashSet<>();

        usedParams.add(params.linkMaxTokens(node, "max_tokens"));
        usedParams.add(params.linkTemperature(node, "temperature"));
        usedParams.add(params.linkTopP(node, "top_p"));
        usedParams.add(params.linkStopSequences(node, "stop"));
        params.linkSystemPrompt(node, "content");
        params.linkModelVersion(node, "model", "gpt-3.5-turbo");
        usedParams.add(params.linkParam(node, "extra", "foo", "extra", "extra2"));
        usedParams.add(params.linkParam(node, "extra3", "extra3"));
        usedParams.add(params.linkDefaultParam(node, "extra4", "unusedDefault", "extra4"));
        usedParams.add(params.linkParam(node, "duplicate", "duplicate1"));
        usedParams.add(params.linkDefaultParam(node, "param1", "defaultVal", "unsetparam"));

        assertThat(node.get("param1").asText()).isEqualTo("defaultVal");

        assertThat(usedParams)
                .containsExactlyInAnyOrder(
                        "maxtokenstosample",
                        "temperature",
                        "p",
                        "stop_sequences",
                        "extra",
                        "extra3",
                        "extra4",
                        "duplicate1",
                        null);

        ObjectNode node2 = mapper.createObjectNode();
        params.linkAllParamsExcept(node2, usedParams);
        // node2 should have all the params we didn't previously link.
        assertThat(node2.size()).isEqualTo(12);
        assertThat(node2.get("SYSTEM_PROMPT").asText()).isEqualTo("ignored");
        assertThat(node2.get("TOP_k").asInt()).isEqualTo(10);
        assertThat(node2.get("max_k").asInt()).isEqualTo(11);
        assertThat(node2.get("EXTRA2").isArray()).isTrue();
        assertThat(node2.get("parameter").isObject()).isTrue();
        assertThat(node2.get("parameter").get("one").asInt()).isEqualTo(1);
        assertThat(node2.get("parameter").get("two").isObject()).isTrue();
        assertThat(node2.get("parameter").get("two").get("three").asInt()).isEqualTo(2);
        assertThat(node2.get("parameter.two").asInt()).isEqualTo(3);
        assertThat(node2.get("parameter").get("two.").get("three").asInt()).isEqualTo(4);
        assertThat(node2.get("parameter").get("two..three").asInt()).isEqualTo(5);
        assertThat(node2.get("parameter").get("two.").get("four").get("five").asInt()).isEqualTo(6);
        assertThat(node2.get("parameter").get("two..four").get("five").asInt()).isEqualTo(7);
        assertThat(node2.get("parameter").get("six").asInt()).isEqualTo(8);
        assertThat(node2.get(".parameter").get("seven").asInt()).isEqualTo(9);
        assertThat(node2.get(".parameter").get("eight").asInt()).isEqualTo(10);
        assertThat(node2.get(".parameter").get("eight.").asInt()).isEqualTo(11);
        assertThat(node2.get(".parameter").get("nine.").asInt()).isEqualTo(12);
        assertThat(node2.get(".parameter").get("nine..").asInt()).isEqualTo(14);
        assertThat(node2.get("...").asInt()).isEqualTo(15);
        assertThat(node2.get(".parameter").get("nine....nine").asInt()).isEqualTo(16);
        assertThat(node2.get("....").asInt()).isEqualTo(17);
        assertThat(node2.get("").asInt()).isEqualTo(13);
        ArrayNode extraSequences = (ArrayNode) node2.get("EXTRA2");
        assertThat(extraSequences.size()).isEqualTo(3);
        assertThat(extraSequences.get(0).asText()).isEqualTo("extra");
        assertThat(extraSequences.get(1).asText()).isEqualTo("extra");
        assertThat(extraSequences.get(2).asText()).isEqualTo("extra");
        assertThat(node2.get("EXTRA5").asBoolean()).isTrue();
        assertThat(node2.get("EXTRA6").asText()).isEqualTo("true");
    }

    private static TextGenerationParams getTextGenerationParams() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("PROVIDER", "BEDROCK");
        modelOptions.put("BEDROCK.PARAMS.TemperaturE", "0.5");
        modelOptions.put("BEDROCK.PARAMS.TOP_k", "10");
        modelOptions.put("BEDROCK.PARAMS.max_k", "11");
        modelOptions.put("BEDROCK.PARAMS.P.FLOAT", "0.9");
        modelOptions.put("BEDROCK.ParamS.maxtokenstosample", "100");
        modelOptions.put("BEDROCK.params.STOP_SEQUENCEs.csv", "stop1,stop2");
        modelOptions.put("BEDROCK.params.EXTRA", "1");
        modelOptions.put("BEDROCK.params.EXTRA2.CSV", "extra,extra,extra");
        modelOptions.put("BEDROCK.params.EXTRA3.int", "123");
        modelOptions.put("BEDROCK.params.EXTRA4.FLOAT", "123");
        modelOptions.put("BEDROCK.params.EXTRA5.bool", "true");
        modelOptions.put("BEDROCK.params.EXTRA6.STRING", "true");
        modelOptions.put("BEDROCK.params.DUPLICATE1", "hi");
        modelOptions.put("BEDROCK.params.DUPLICATE1", "hello");
        modelOptions.put("BEDROCK.params.Duplicate1", "hello again");
        modelOptions.put("BEDROCK.params.DUPLICATE1.INT", "123");
        modelOptions.put("BEDROCK.params.parameter.one.INT", "1");
        modelOptions.put("BEDROCK.params.parameter.two.three", "2");
        modelOptions.put("BEDROCK.params.parameter..two", "3");
        modelOptions.put("BEDROCK.params.parameter.two...three", "4");
        modelOptions.put("BEDROCK.params.parameter.two....three", "5");
        modelOptions.put("BEDROCK.params.parameter.two...four.five", "6");
        modelOptions.put("BEDROCK.params.parameter.two....four.five", "7");
        modelOptions.put("BEDROCK.params..parameter.six", "8");
        modelOptions.put("BEDROCK.params...parameter.seven", "9");
        modelOptions.put("BEDROCK.params...parameter.eight.", "10");
        modelOptions.put("BEDROCK.params...parameter.eight..", "11");
        modelOptions.put("BEDROCK.params...parameter.nine...", "12");
        modelOptions.put("BEDROCK.params...parameter.nine....", "14");
        modelOptions.put("BEDROCK.params.......", "15");
        modelOptions.put("BEDROCK.params...parameter.nine........nine", "16");
        modelOptions.put("BEDROCK.params..........", "17");
        modelOptions.put("BEDROCK.params.", "13");
        modelOptions.put("BEDROCK.params.SYSTEM_PROMPT", "ignored");
        modelOptions.put("BEDROCK.SYSTEM_prompt", "System Prompt!");
        modelOptions.put("BEDROCK.model_VERSION", "model-123");
        return new TextGenerationParams(modelOptions);
    }
}
