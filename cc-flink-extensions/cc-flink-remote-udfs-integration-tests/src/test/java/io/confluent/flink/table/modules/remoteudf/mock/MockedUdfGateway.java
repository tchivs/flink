/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.mock;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.modules.remoteudf.util.TestUtils;
import io.confluent.flink.udf.adapter.api.RemoteUdfSerialization;
import io.confluent.flink.udf.adapter.api.RemoteUdfSpec;
import io.confluent.secure.compute.gateway.v1.Error;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionRequest;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionResponse;
import io.confluent.secure.compute.gateway.v1.SecureComputeGatewayGrpc;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** Mock implementation of the UDF gateway. */
public class MockedUdfGateway extends SecureComputeGatewayGrpc.SecureComputeGatewayImplBase {
    private final AtomicReference<RemoteUdfSpec> testUdfSpecReference = new AtomicReference<>();
    private boolean batch;

    @Override
    public void invokeFunction(
            InvokeFunctionRequest request,
            StreamObserver<InvokeFunctionResponse> responseObserver) {

        RemoteUdfSpec testUdfSpec = testUdfSpecReference.get();
        Preconditions.checkNotNull(testUdfSpec);

        InvokeFunctionResponse.Builder builder = InvokeFunctionResponse.newBuilder();

        try {
            List<TypeSerializer<Object>> argumentSerializers =
                    testUdfSpec.createArgumentSerializers();
            RemoteUdfSerialization serialization =
                    new RemoteUdfSerialization(
                            testUdfSpec.createReturnTypeSerializer(), argumentSerializers);

            List<Object[]> argumentsBatch = null;
            Object[] args = new Object[argumentSerializers.size()];
            if (batch) {
                argumentsBatch =
                        serialization.deserializeBatchArguments(
                                request.getPayload().asReadOnlyByteBuffer());
            } else {
                serialization.deserializeArguments(
                        request.getPayload().asReadOnlyByteBuffer(), args);
            }
            if (testUdfSpec
                    .getReturnType()
                    .getLogicalType()
                    .is(DataTypes.STRING().getLogicalType().getTypeRoot())) {
                if (batch) {
                    List<Object> results =
                            argumentsBatch.stream()
                                    .map(
                                            a ->
                                                    BinaryStringData.fromString(
                                                            "str:" + Arrays.asList(a)))
                                    .collect(Collectors.toList());

                    builder.setPayload(serialization.serializeBatchReturnValue(results));
                } else {
                    builder.setPayload(
                            serialization.serializeReturnValue(
                                    BinaryStringData.fromString("str:" + Arrays.asList(args))));
                }
            } else if (testUdfSpec
                    .getReturnType()
                    .getLogicalType()
                    .is(DataTypes.INT().getLogicalType().getTypeRoot())) {
                if (batch) {
                    AtomicInteger i = new AtomicInteger(0);
                    List<Object> results =
                            argumentsBatch.stream()
                                    .map(a -> i.getAndIncrement())
                                    .collect(Collectors.toList());
                    builder.setPayload(serialization.serializeBatchReturnValue(results));
                } else {
                    builder.setPayload(
                            serialization.serializeReturnValue(
                                    TestUtils.EXPECTED_INT_RETURN_VALUE));
                }

            } else {
                throw new Exception(
                        "Unknown return type " + testUdfSpec.getReturnType().getLogicalType());
            }
        } catch (Exception ex) {
            builder.setError(Error.newBuilder().setCode(1).setMessage(ex.getMessage()).build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    public void registerUdfSpec(RemoteUdfSpec udfSpec, boolean batch) {
        this.batch = batch;
        testUdfSpecReference.set(udfSpec);
    }
}
