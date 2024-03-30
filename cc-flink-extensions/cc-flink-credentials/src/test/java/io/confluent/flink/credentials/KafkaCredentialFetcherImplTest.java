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
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialV2;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentials;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialRequestV2;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialResponseV2;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaCredentialFetcherImpl}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class KafkaCredentialFetcherImplTest {

    private static final long DEFAULT_DEADLINE_MS = 1000;

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
                        JobID.generate(),
                        "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                        "computePoolId",
                        new ArrayList<>(),
                        false,
                        0,
                        10);
        decrypter = new MockCredentialDecrypter();
        fetcher =
                new KafkaCredentialFetcherImpl(
                        flinkCredentialService, dpatTokenExchanger, decrypter, DEFAULT_DEADLINE_MS);
        handler.withResponse(
                        GetCredentialsResponse.newBuilder()
                                .setFlinkCredentials(
                                        FlinkCredentials.newBuilder()
                                                .setApiKey("api_key")
                                                .setEncryptedSecret(
                                                        ByteString.copyFrom(
                                                                "secret", StandardCharsets.UTF_8))
                                                .build())
                                .build())
                .withResponseV2(
                        GetCredentialResponseV2.newBuilder()
                                .setFlinkCredentials(
                                        FlinkCredentialV2.newBuilder()
                                                .setApiKey("api_key")
                                                .setEncryptedSecret(
                                                        ByteString.copyFrom(
                                                                "secret", StandardCharsets.UTF_8))
                                                .build())
                                .build());
        decrypter.withDecryptedResult("decrypted_secret".getBytes());
        dpatTokenExchanger.withToken(new DPATTokens("token"));
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
    public void testFetch_success_sa_principals() {
        List<String> saPrincipals =
                Arrays.stream(new String[] {"sa-123"}).collect(Collectors.toList());
        JobCredentialsMetadata saJobCredentialsMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(),
                        "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                        "computePoolId",
                        saPrincipals,
                        false,
                        0,
                        10);
        KafkaCredentials kafkaCredentials = fetcher.fetchToken(saJobCredentialsMetadata);
        assertThat(kafkaCredentials.getDpatToken()).isEqualTo("token");
    }

    @Test
    public void testFetch_success_user_principals() {
        JobCredentialsMetadata userJobCredentialMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(),
                        "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                        "computePoolId",
                        Collections.singletonList("u-123"),
                        false,
                        0,
                        10);
        KafkaCredentials kafkaCredentials = fetcher.fetchToken(userJobCredentialMetadata);
        assertThat(kafkaCredentials.getDpatToken()).isEqualTo("token");
    }

    @Test
    public void testFetch_success_user_principals_pools() {
        List<String> userAndIdentityPoolPrincipals =
                Arrays.stream(new String[] {"u-123", "pool-123", "pool-234"})
                        .collect(Collectors.toList());
        JobCredentialsMetadata userJobCredentialMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(),
                        "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                        "computePoolId",
                        userAndIdentityPoolPrincipals,
                        false,
                        0,
                        10);
        KafkaCredentials kafkaCredentials = fetcher.fetchToken(userJobCredentialMetadata);
        assertThat(kafkaCredentials.getDpatToken()).isEqualTo("token");
    }

    @Test
    public void testFetch_success_withUDFs() {
        dpatTokenExchanger.withToken(new DPATTokens("token", Optional.of("udf_token")));
        JobCredentialsMetadata userJobCredentialMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(),
                        "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                        "computePoolId",
                        Collections.singletonList("u-123"),
                        true,
                        0,
                        10);
        KafkaCredentials kafkaCredentials = fetcher.fetchToken(userJobCredentialMetadata);
        assertThat(kafkaCredentials.getDpatToken()).isEqualTo("token");
        assertThat(kafkaCredentials.getUdfDpatToken()).contains("udf_token");
    }

    @Test
    public void testFetch_failGetCredentials() {
        handler.withError();
        assertThatThrownBy(() -> fetcher.fetchToken(jobCredentialsMetadata))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to do credential request");
    }

    @Test
    public void testFetch_failGetCredentials_user_pools() {
        List<String> userAndIdentityPoolPrincipals =
                Arrays.stream(new String[] {"u-123", "pool-123", "pool-234"})
                        .collect(Collectors.toList());
        JobCredentialsMetadata userJobCredentialMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(),
                        "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                        "computePoolId",
                        userAndIdentityPoolPrincipals,
                        false,
                        0,
                        10);
        handler.withError();
        assertThatThrownBy(() -> fetcher.fetchToken(userJobCredentialMetadata))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to do credential request");
    }

    @Test
    public void testFetch_error_null_jobcredentialmetadatada() {
        assertThatThrownBy(() -> fetcher.fetchToken(null))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to do credential request");
    }

    @Test
    public void testFetch_failGetCredentials_user() {
        List<String> userPrincipals =
                Arrays.stream(new String[] {"u-123"}).collect(Collectors.toList());
        JobCredentialsMetadata userJobCredentialMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(),
                        "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                        "computePoolId",
                        userPrincipals,
                        false,
                        0,
                        10);
        handler.withError();
        assertThatThrownBy(() -> fetcher.fetchToken(userJobCredentialMetadata))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to do credential request");
    }

    @Test
    public void testFetch_failGetCredentials_sa() {
        List<String> userPrincipals =
                Arrays.stream(new String[] {"sa-123"}).collect(Collectors.toList());
        JobCredentialsMetadata userJobCredentialMetadata =
                new JobCredentialsMetadata(
                        JobID.generate(),
                        "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                        "computePoolId",
                        userPrincipals,
                        false,
                        0,
                        10);
        handler.withError();
        assertThatThrownBy(() -> fetcher.fetchToken(userJobCredentialMetadata))
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
                .hasMessageContaining("Failed to do fetch DPAT Token");
    }

    @Test
    public void testFetch_deadline() {
        fetcher =
                new KafkaCredentialFetcherImpl(
                        flinkCredentialService, dpatTokenExchanger, decrypter, 10);
        handler.withDelayedResponse(50);
        assertThatThrownBy(() -> fetcher.fetchToken(jobCredentialsMetadata))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to do credential request for JobCredentials")
                .cause()
                .hasMessageContaining("DEADLINE_EXCEEDED");
    }

    /** The handler for the fake RPC server. */
    public static class Handler extends FlinkCredentialServiceGrpc.FlinkCredentialServiceImplBase {

        private GetCredentialsResponse response;
        private GetCredentialResponseV2 responseV2;
        private boolean error;
        private long delayMs = -1;

        public Handler() {}

        public Handler withError() {
            this.error = true;
            return this;
        }

        public Handler withDelayedResponse(long delayMs) {
            this.delayMs = delayMs;
            return this;
        }

        public Handler withResponse(GetCredentialsResponse response) {
            this.response = response;
            return this;
        }

        public Handler withResponseV2(GetCredentialResponseV2 response) {
            this.responseV2 = response;
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
            if (delayMs >= 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                }
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getCredentialV2(
                GetCredentialRequestV2 request,
                StreamObserver<GetCredentialResponseV2> responseObserver) {
            if (error) {
                responseObserver.onError(new RuntimeException("Server Error!"));
                return;
            }
            if (delayMs >= 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                }
            }
            responseObserver.onNext(responseV2);
            responseObserver.onCompleted();
        }
    }
}
