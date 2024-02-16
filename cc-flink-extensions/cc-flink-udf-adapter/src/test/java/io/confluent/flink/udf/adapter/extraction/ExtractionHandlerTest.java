/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.extraction;

import io.confluent.flink.udf.adapter.examples.OverloadedSumScalarFunction;
import io.confluent.flink.udf.adapter.examples.SumScalarFunction;
import io.confluent.flink.udf.extractor.Extractor;
import org.junit.jupiter.api.Test;

import static io.confluent.flink.udf.adapter.TestUtil.DUMMY_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link ExtractionHandler}. */
public class ExtractionHandlerTest {

    @Test
    public void testExtractSumFunction() throws Throwable {
        final ExtractionHandler extractionHandler = new ExtractionHandler();
        extractionHandler.open(new byte[0], DUMMY_CONTEXT);
        Extractor.ExtractionRequest.Builder request = Extractor.ExtractionRequest.newBuilder();
        request.setPluginId("abc")
                .setPluginVersionId("xyz")
                .setClassName(SumScalarFunction.class.getName())
                .build();
        byte[] bytes =
                extractionHandler.handleRequest(request.build().toByteArray(), DUMMY_CONTEXT);
        extractionHandler.close(new byte[0], DUMMY_CONTEXT);

        Extractor.ExtractionResponse response = Extractor.ExtractionResponse.parseFrom(bytes);
        assertThat(response.getSignaturesCount()).isEqualTo(1);
        assertThat(response.getSignatures(0).getArgumentTypesList()).containsExactly("INT", "INT");
        assertThat(response.getSignatures(0).getReturnType()).isEqualTo("INT");
    }

    @Test
    public void testOverloadedSumFunction() throws Throwable {
        final ExtractionHandler extractionHandler = new ExtractionHandler();
        extractionHandler.open(new byte[0], DUMMY_CONTEXT);
        Extractor.ExtractionRequest.Builder request = Extractor.ExtractionRequest.newBuilder();
        request.setPluginId("abc")
                .setPluginVersionId("xyz")
                .setClassName(OverloadedSumScalarFunction.class.getName())
                .build();
        byte[] bytes =
                extractionHandler.handleRequest(request.build().toByteArray(), DUMMY_CONTEXT);
        extractionHandler.close(new byte[0], DUMMY_CONTEXT);

        Extractor.ExtractionResponse response = Extractor.ExtractionResponse.parseFrom(bytes);
        assertThat(response.getSignaturesCount()).isEqualTo(3);
        assertThat(response.getSignatures(0).getArgumentTypesList()).containsExactly("INT", "INT");
        assertThat(response.getSignatures(0).getReturnType()).isEqualTo("INT");
        assertThat(response.getSignatures(1).getArgumentTypesList())
                .containsExactly("INT", "INT", "INT");
        assertThat(response.getSignatures(1).getReturnType()).isEqualTo("INT");
        assertThat(response.getSignatures(2).getArgumentTypesList())
                .containsExactly("INT", "INT", "INT", "INT");
        assertThat(response.getSignatures(2).getReturnType()).isEqualTo("INT");
    }
}
