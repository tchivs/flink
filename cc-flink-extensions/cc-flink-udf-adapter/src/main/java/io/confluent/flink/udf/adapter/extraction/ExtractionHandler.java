/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.extraction;

import org.apache.flink.table.types.extraction.ConfluentUdfExtractor;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.flink.udf.extractor.Extractor;
import io.confluent.flink.udf.extractor.Extractor.ExtractionRequest;
import io.confluent.flink.udf.extractor.Extractor.ExtractionResponse;
import io.confluent.function.runtime.core.Context;
import io.confluent.function.runtime.core.RequestHandler;
import io.confluent.function.runtime.core.RequestInvocationException;

import java.util.logging.Level;
import java.util.logging.Logger;

/** Handles calls to extract metadata from a UDF class. */
public class ExtractionHandler implements RequestHandler {
    private static final Logger LOG = Logger.getLogger(ExtractionHandler.class.getName());

    public static final int ERROR_CODE_BAD_MESSAGE = 101;

    @Override
    public byte[] handleRequest(byte[] bytes, Context context) throws RequestInvocationException {
        ExtractionRequest request;
        try {
            request = ExtractionRequest.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            LOG.log(Level.SEVERE, "Error parsing request", e);
            throw new RequestInvocationException(ERROR_CODE_BAD_MESSAGE, "Error parsing request");
        }
        LOG.log(Level.INFO, "Extraction request: " + request);
        ExtractionResponse.Builder builder = ExtractionResponse.newBuilder();
        try {
            Metadata metadata =
                    MetadataExtractor.extract(
                            this.getClass().getClassLoader(), request.getClassName());
            for (ConfluentUdfExtractor.Signature signature : metadata.getSignatures()) {
                builder.addSignatures(
                        Extractor.Signature.newBuilder()
                                .addAllArgumentTypes(signature.getSerializedArgumentTypes())
                                .setReturnType(signature.getSerializedReturnType())
                                .build());
            }
        } catch (MetadataExtractionException e) {
            LOG.log(Level.WARNING, "An error occurred during extraction", e);
            builder.setError(
                    Extractor.Error.newBuilder()
                            .setCode(e.getCode())
                            .setErrorMessage(e.getMessage()));
        } catch (Throwable e) {
            LOG.log(Level.SEVERE, "An unknown error occurred during extraction", e);
            builder.setError(
                    Extractor.Error.newBuilder()
                            .setCode(Extractor.ErrorCode.UNKNOWN)
                            .setErrorMessage("An internal error occurred during extraction."));
        }
        ExtractionResponse response = builder.build();
        LOG.log(Level.INFO, "Extraction response: " + response);
        return response.toByteArray();
    }

    @Override
    public void open(byte[] bytes, Context context) {
        LOG.log(Level.INFO, "Opening Extraction");
    }

    @Override
    public void close(byte[] bytes, Context context) {
        LOG.log(Level.INFO, "Closing Extraction");
    }
}
