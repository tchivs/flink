/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.TestLoggerExtension;

import cloud.confluent.cc_auth_dataplane.v1.AuthDataplaneServiceGrpc;
import cloud.confluent.cc_auth_dataplane.v1.IssueFlinkAuthTokenRequest;
import cloud.confluent.cc_auth_dataplane.v1.IssueFlinkAuthTokenResponse;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataplaneTokenExchanger}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class DataplaneTokenExchangerTest {

    private static final JobCredentialsMetadata SA_PRINCIPAL_METADATA =
            new JobCredentialsMetadata(
                    JobID.generate(),
                    "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                    "computepool",
                    Collections.singletonList("sa-123"),
                    false,
                    0,
                    0);

    private static final JobCredentialsMetadata USER_PRINCIPAL_METADATA =
            new JobCredentialsMetadata(
                    JobID.generate(),
                    "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                    "computepool",
                    Collections.singletonList("u-123"),
                    false,
                    0,
                    0);

    private static final JobCredentialsMetadata USER_POOL_PRINCIPAL_METADATA =
            new JobCredentialsMetadata(
                    JobID.generate(),
                    "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                    "computepool",
                    Arrays.stream(new String[] {"u-123", "pool-123", "pool-234", "group-123"})
                            .collect(Collectors.toList()),
                    false,
                    0,
                    0);

    private static final JobCredentialsMetadata UDF_METADATA =
            new JobCredentialsMetadata(
                    JobID.generate(),
                    "crn://confluent.cloud/organization=e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment=env-xx5q1x/flink-region=aws.us-west-2/statement=cl-jvu-1694189115-kafka2.0",
                    "computepool",
                    Collections.singletonList("sa-123"),
                    true,
                    0,
                    0);

    private static final String STATEMENT_CRN =
            "crn://confluent.cloud/organization="
                    + "e9eb4f2c-ef73-475c-ba7f-6b37a4ff00e5/environment="
                    + "env-xx5q1x/flink-region=aws.us-west-2/statement="
                    + "cl-jvu-1694189115-kafka2.0";

    private static final Pair<String, String> STATIC_CREDS = Pair.of("key", "secret");

    private DataplaneTokenExchanger tokenExchanger;
    private Server server;
    private ManagedChannel channel;
    private Handler handler;
    private AuthDataplaneServiceGrpc.AuthDataplaneServiceBlockingStub authDataplaneService;

    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @BeforeEach
    public void setUp() throws IOException {
        handler = new Handler();
        String uniqueName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(uniqueName).addService(handler).build();
        server.start();
        channel =
                grpcCleanup.register(
                        InProcessChannelBuilder.forName(uniqueName).directExecutor().build());
        authDataplaneService = AuthDataplaneServiceGrpc.newBlockingStub(channel);

        tokenExchanger = new DataplaneTokenExchanger(authDataplaneService, 10000);
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
    public void testFetch_service_account_principal() {
        handler.withResponse(IssueFlinkAuthTokenResponse.newBuilder().setToken("abc").build());
        DPATTokens token = tokenExchanger.fetch(STATIC_CREDS, SA_PRINCIPAL_METADATA);
        assertThat(handler.getRequest().getServiceAccountId()).isEqualTo("sa-123");
        assertThat(token.getToken()).isEqualTo("abc");
        assertThat(token.getUDFToken()).isEmpty();
    }

    @Test
    public void testFetch_user_principal() {
        handler.withResponse(IssueFlinkAuthTokenResponse.newBuilder().setToken("abc").build());
        DPATTokens token = tokenExchanger.fetch(STATIC_CREDS, USER_PRINCIPAL_METADATA);
        assertThat(handler.getRequest().getUserResourceId()).isEqualTo("u-123");
        AssertionsForClassTypes.assertThat(token.getToken()).isEqualTo("abc");
        assertThat(token.getUDFToken()).isEmpty();
    }

    @Test
    public void testFetch_user_pools_principal() {
        handler.withResponse(IssueFlinkAuthTokenResponse.newBuilder().setToken("abc").build());
        DPATTokens token = tokenExchanger.fetch(STATIC_CREDS, USER_POOL_PRINCIPAL_METADATA);
        assertThat(handler.getRequest().getUserResourceId()).isEqualTo("u-123");
        assertThat(handler.getRequest().getIdentityPoolIdsCount()).isEqualTo(3);
        assertThat(handler.getRequest().getIdentityPoolIds(0)).isEqualTo("pool-123");
        assertThat(handler.getRequest().getIdentityPoolIds(1)).isEqualTo("pool-234");
        assertThat(handler.getRequest().getIdentityPoolIds(2)).isEqualTo("group-123");
        assertThat(token.getToken()).isEqualTo("abc");
        assertThat(token.getUDFToken()).isEmpty();
    }

    @Test
    public void testUdfs() {
        handler.withResponse(IssueFlinkAuthTokenResponse.newBuilder().setToken("abc").build());
        handler.withResponse(IssueFlinkAuthTokenResponse.newBuilder().setToken("def").build());
        DPATTokens token = tokenExchanger.fetch(STATIC_CREDS, UDF_METADATA);
        assertThat(handler.getRequest(0).getServiceAccountId()).isEqualTo("sa-123");
        assertThat(handler.getRequest(0).getTarget()).isEmpty();
        assertThat(handler.getRequest(1).getServiceAccountId()).isEqualTo("sa-123");
        assertThat(handler.getRequest(1).getTarget()).isEqualTo("cc-secure-compute-gateway");
        assertThat(token.getToken()).isEqualTo("abc");
        assertThat(token.getUDFToken()).contains("def");
    }

    @Test
    public void testFetch_internalError() {
        handler.withError();
        assertThatThrownBy(() -> tokenExchanger.fetch(STATIC_CREDS, USER_PRINCIPAL_METADATA))
                .cause()
                .hasMessageContaining("Server Error!");
    }

    @Test
    public void testFetch_deadline() {
        tokenExchanger = new DataplaneTokenExchanger(authDataplaneService, 10);
        handler.withDelayedResponse(50);
        assertThatThrownBy(() -> tokenExchanger.fetch(STATIC_CREDS, USER_PRINCIPAL_METADATA))
                .cause()
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED");
    }

    /** The handler for the fake RPC server. */
    private static class Handler extends AuthDataplaneServiceGrpc.AuthDataplaneServiceImplBase {

        private final List<IssueFlinkAuthTokenRequest> requests = new ArrayList<>();
        private final LinkedList<IssueFlinkAuthTokenResponse> responses = new LinkedList<>();
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

        public Handler withResponse(IssueFlinkAuthTokenResponse response) {
            this.responses.add(response);
            return this;
        }

        @Override
        public void issueFlinkAuthToken(
                IssueFlinkAuthTokenRequest request,
                StreamObserver<IssueFlinkAuthTokenResponse> responseObserver) {
            this.requests.add(request);
            if (error) {
                Status status =
                        Status.newBuilder()
                                .setCode(Code.INTERNAL.getNumber())
                                .setMessage("Server Error!")
                                .build();
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
                return;
            }
            if (delayMs >= 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                }
            }
            responseObserver.onNext(responses.pop());
            responseObserver.onCompleted();
        }

        public IssueFlinkAuthTokenRequest getRequest() {
            return requests.stream().findFirst().orElse(null);
        }

        public IssueFlinkAuthTokenRequest getRequest(int i) {
            return requests.get(i);
        }
    }
}
