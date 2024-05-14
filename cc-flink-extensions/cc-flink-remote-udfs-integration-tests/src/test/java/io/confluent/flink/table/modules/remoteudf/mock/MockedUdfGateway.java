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
import java.util.concurrent.atomic.AtomicReference;

/** Mock implementation of the UDF gateway. */
public class MockedUdfGateway extends SecureComputeGatewayGrpc.SecureComputeGatewayImplBase {
    private final AtomicReference<RemoteUdfSpec> testUdfSpecReference = new AtomicReference<>();

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
            Object[] args = new Object[argumentSerializers.size()];
            serialization.deserializeArguments(request.getPayload().asReadOnlyByteBuffer(), args);
            if (testUdfSpec
                    .getReturnType()
                    .getLogicalType()
                    .is(DataTypes.STRING().getLogicalType().getTypeRoot())) {
                builder.setPayload(
                        serialization.serializeReturnValue(
                                BinaryStringData.fromString("str:" + Arrays.asList(args))));
            } else if (testUdfSpec
                    .getReturnType()
                    .getLogicalType()
                    .is(DataTypes.INT().getLogicalType().getTypeRoot())) {
                builder.setPayload(
                        serialization.serializeReturnValue(TestUtils.EXPECTED_INT_RETURN_VALUE));
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

    public void registerUdfSpec(RemoteUdfSpec udfSpec) {
        testUdfSpecReference.set(udfSpec);
    }
}
