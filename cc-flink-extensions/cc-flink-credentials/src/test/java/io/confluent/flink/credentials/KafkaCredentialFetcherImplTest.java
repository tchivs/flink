/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLoggerExtension;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentials;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialsRequest;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialsResponse;
import com.google.protobuf.ByteString;
import io.confluent.flink.credentials.utils.MockCredentialDecrypter;
import io.confluent.flink.credentials.utils.MockTokenExchanger;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaCredentialFetcherImpl}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class KafkaCredentialFetcherImplTest {

    private FlinkCredentialServiceBlockingStub flinkCredentialService;
    private MockTokenExchanger dpatTokenExchanger;
    private MockCredentialDecrypter decrypter;
    private Server server;
    private ManagedChannel channel;
    private Handler handler;

    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private JobCredentialsMetadata jobCredentialsMetadata;
    private KafkaCredentialFetcherImpl fetcher;

    @BeforeEach
    public void setUp() throws IOException {
        dpatTokenExchanger = new MockTokenExchanger();
        handler = new Handler();
        String uniqueName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(uniqueName).addService(handler).build();
        server.start();
        channel =
                grpcCleanup.register(
                        InProcessChannelBuilder.forName(uniqueName).directExecutor().build());
        flinkCredentialService = FlinkCredentialServiceGrpc.newBlockingStub(channel);
        jobCredentialsMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(), "statementId", "computePoolId", "identityPoolId", 0, 10);
        decrypter = new MockCredentialDecrypter();
        fetcher =
                new KafkaCredentialFetcherImpl(
                        flinkCredentialService, dpatTokenExchanger, decrypter);
        handler.withResponse(
                GetCredentialsResponse.newBuilder()
                        .setFlinkCredentials(
                                FlinkCredentials.newBuilder()
                                        .setApiKey("api_key")
                                        .setEncryptedSecret(
                                                ByteString.copyFrom(
                                                        "secret", StandardCharsets.UTF_8))
                                        .build())
                        .build());
        decrypter.withDecryptedResult("decrypted_secret".getBytes());
        dpatTokenExchanger.withToken(new DPATToken("token"));
    }

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.shutdownNow();
        }
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    @Test
    public void testFetch_success() {
        KafkaCredentials kafkaCredentials = fetcher.fetchToken(jobCredentialsMetadata);
        assertThat(kafkaCredentials.getDpatToken()).isEqualTo("token");
    }

    @Test
    public void testFetch_failGetCredentials() {
        handler.withError();
        assertThatThrownBy(() -> fetcher.fetchToken(jobCredentialsMetadata))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to do credential request");
    }

    @Test
    public void testFetch_decryptError() {
        decrypter.withError();
        assertThatThrownBy(() -> fetcher.fetchToken(jobCredentialsMetadata))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Decryption Error");
    }

    @Test
    public void testFetch_failGetDPAT() {
        dpatTokenExchanger.withError();
        assertThatThrownBy(() -> fetcher.fetchToken(jobCredentialsMetadata))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to do fetch DPAT Token for compute");
    }

    /** The handler for the fake RPC server. */
    public static class Handler extends FlinkCredentialServiceGrpc.FlinkCredentialServiceImplBase {

        private GetCredentialsResponse response;
        private boolean error;

        public Handler() {}

        public Handler withError() {
            this.error = true;
            return this;
        }

        public Handler withResponse(GetCredentialsResponse response) {
            this.response = response;
            return this;
        }

        @Override
        public void getCredentials(
                GetCredentialsRequest request,
                StreamObserver<GetCredentialsResponse> responseObserver) {
            if (error) {
                responseObserver.onError(new RuntimeException("Server Error!"));
                return;
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
