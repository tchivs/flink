/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.secrets;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import cloud.confluent.ksql_api_service.flinkcredential.KMSDecryptionRequest;
import cloud.confluent.ksql_api_service.flinkcredential.KMSDecryptionResponse;
import com.google.protobuf.ByteString;
import io.confluent.flink.credentials.utils.CallWithRetry;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_HOST;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for FlinkCredentialServiceSecretDecrypter. */
public class FlinkCredentialServiceSecretDecrypterTest {
    private CatalogModel model;
    private Map<String, String> options;
    private FlinkCredentialServiceSecretDecrypter decrypter;
    private Server server;
    private ManagedChannel channel;
    private Handler handler;
    private FlinkCredentialServiceBlockingStub flinkCredentialService;

    @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @BeforeEach
    void setUp() throws IOException {
        Map<String, String> immutableMap =
                ImmutableMap.of(
                        "provider",
                        "openai",
                        "openai.api_key",
                        "api_key",
                        CREDENTIAL_SERVICE_HOST.key(),
                        "localhost",
                        CREDENTIAL_SERVICE_PORT.key(),
                        "123",
                        MLModelCommonConstants.ORG_ID,
                        "org1",
                        MLModelCommonConstants.ENV_ID,
                        "env1",
                        MLModelCommonConstants.DATABASE_ID,
                        "db1",
                        MLModelCommonConstants.MODEL_NAME,
                        "model1",
                        MLModelCommonConstants.MODEL_VERSION,
                        "v1",
                        MLModelCommonConstants.COMPUTE_POOL_ID,
                        "cp1");
        options = new HashMap<>(immutableMap);
        options.put(MLModelCommonConstants.ENCRYPT_STRATEGY, "kms");
        options.put(MLModelCommonConstants.COMPUTE_POOL_ENV_ID, "cp_env1");
        model = modelFromOption(options);
        handler = new Handler();
        String uniqueName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(uniqueName).addService(handler).build();
        server.start();
        channel =
                grpcCleanup.register(
                        InProcessChannelBuilder.forName(uniqueName).directExecutor().build());
        flinkCredentialService = FlinkCredentialServiceGrpc.newBlockingStub(channel);
        decrypter =
                new FlinkCredentialServiceSecretDecrypter(
                        model,
                        flinkCredentialService,
                        data -> data.getBytes(StandardCharsets.UTF_8),
                        new CallWithRetry(3, 100));
    }

    @Test
    public void testStrategy() {
        assertThat(decrypter.supportedStrategy()).isEqualTo(EncryptionStrategy.KMS);
    }

    @Test
    public void testStrategyMatch() {
        options.remove(MLModelCommonConstants.ENCRYPT_STRATEGY);
        model = modelFromOption(options);
        decrypter = new FlinkCredentialServiceSecretDecrypter(model);
        assertThatThrownBy(() -> decrypter.decryptFromKey("openai.api_key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Decrypt key which is not KMS encrypted");
    }

    @Test
    public void testNonExistKey() {
        assertThat(decrypter.decryptFromKey("somekey")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                MLModelCommonConstants.ORG_ID,
                MLModelCommonConstants.COMPUTE_POOL_ID,
                MLModelCommonConstants.COMPUTE_POOL_ENV_ID
            })
    public void testMissRequiredParam(String param) {
        options.remove(param);
        model = modelFromOption(options);
        decrypter = new FlinkCredentialServiceSecretDecrypter(model);
        assertThatThrownBy(() -> decrypter.decryptFromKey("openai.api_key"))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                MLModelCommonConstants.ENV_ID,
                MLModelCommonConstants.DATABASE_ID,
                MLModelCommonConstants.MODEL_NAME,
                MLModelCommonConstants.MODEL_VERSION
            })
    public void testMissOptionalParam(String param) {
        options.remove(param);
        model = modelFromOption(options);
        decrypter =
                new FlinkCredentialServiceSecretDecrypter(
                        model,
                        flinkCredentialService,
                        data -> data.getBytes(StandardCharsets.UTF_8),
                        new CallWithRetry(3, 100));
        handler.withPlainText("decrypted");
        String secret = decrypter.decryptFromKey("openai.api_key");
        assertThat(secret).isEqualTo("decrypted");
    }

    @Test
    public void testDecryptionFailureWithRetry() {
        handler.withPlainText("decrypted");
        handler.withMaxErrorCount(4);
        assertThatThrownBy(() -> decrypter.decryptFromKey("openai.api_key"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testDecryptionWithRetry() {
        handler.withPlainText("decrypted");
        handler.withMaxErrorCount(3);
        String secret = decrypter.decryptFromKey("openai.api_key");
        assertThat(secret).isEqualTo("decrypted");
    }

    private static CatalogModel modelFromOption(Map<String, String> options) {
        return CatalogModel.of(
                Schema.newBuilder().build(), Schema.newBuilder().build(), options, null);
    }

    /** The handler for the fake RPC server. */
    public static class Handler extends FlinkCredentialServiceGrpc.FlinkCredentialServiceImplBase {

        private String plainText;
        private int maxErrorCount;
        private AtomicInteger retryCounter = new AtomicInteger(0);

        public Handler() {}

        public Handler withPlainText(String plainText) {
            this.plainText = plainText;
            return this;
        }

        public Handler withMaxErrorCount(int errorCount) {
            maxErrorCount = errorCount;
            return this;
        }

        @Override
        public void decryptSecret(
                KMSDecryptionRequest request,
                StreamObserver<KMSDecryptionResponse> responseObserver) {
            if (maxErrorCount > 0 && retryCounter.incrementAndGet() < maxErrorCount) {
                responseObserver.onError(new RuntimeException("Server Error!"));
                return;
            }
            if (plainText != null) {
                responseObserver.onNext(
                        KMSDecryptionResponse.newBuilder()
                                .setPlainText(ByteString.copyFromUtf8(plainText))
                                .build());
                responseObserver.onCompleted();
            }
        }
    }
}
