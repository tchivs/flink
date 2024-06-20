/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.secrets;

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
import io.confluent.flink.table.utils.secrets.FlinkCredentialServiceSecretDecrypter.ConfigAnnotation;
import io.confluent.flink.table.utils.secrets.FlinkCredentialServiceSecretDecrypter.ParsedConfig;
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
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for FlinkCredentialServiceSecretDecrypter. */
public class ModelSecretDecrypterTest {
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
                        MLModelCommonConstants.CREDENTIAL_SERVICE_HOST,
                        "localhost",
                        MLModelCommonConstants.CREDENTIAL_SERVICE_PORT,
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
                new ModelSecretDecrypter(
                        model,
                        ImmutableMap.of(),
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
        options.put(MLModelCommonConstants.ENCRYPT_STRATEGY, "invalid");
        model = modelFromOption(options);
        decrypter = new ModelSecretDecrypter(model, ImmutableMap.of());
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
                MLModelCommonConstants.COMPUTE_POOL_ENV_ID,
                MLModelCommonConstants.CREDENTIAL_SERVICE_HOST,
                MLModelCommonConstants.CREDENTIAL_SERVICE_PORT,
                MLModelCommonConstants.ENCRYPT_STRATEGY
            })
    public void testMissRequiredParam(String param) {
        options.remove(param);
        model = modelFromOption(options);
        if (param.equals(MLModelCommonConstants.CREDENTIAL_SERVICE_PORT)) {
            assertThatThrownBy(() -> new ModelSecretDecrypter(model, ImmutableMap.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("CONFLUENT.CREDENTIAL.SERVICE.PORT should be a number");
        } else {
            assertThatThrownBy(() -> new ModelSecretDecrypter(model, ImmutableMap.of()))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                MLModelCommonConstants.ENV_ID,
                MLModelCommonConstants.DATABASE_ID,
                MLModelCommonConstants.MODEL_NAME,
                MLModelCommonConstants.MODEL_VERSION,
                MLModelCommonConstants.KMS_SECRET_RETRY_COUNT,
                MLModelCommonConstants.KMS_SECRET_RETRY_DELAY_MS
            })
    public void testMissOptionalParam(String param) {
        options.remove(param);
        model = modelFromOption(options);
        decrypter =
                new ModelSecretDecrypter(
                        model,
                        ImmutableMap.of(),
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

    @ParameterizedTest
    @ValueSource(
            strings = {
                // Required config
                MLModelCommonConstants.ORG_ID,
                MLModelCommonConstants.COMPUTE_POOL_ID,
                MLModelCommonConstants.COMPUTE_POOL_ENV_ID,
                MLModelCommonConstants.CREDENTIAL_SERVICE_HOST,
                MLModelCommonConstants.CREDENTIAL_SERVICE_PORT,

                // Optional config
                MLModelCommonConstants.ENV_ID,
                MLModelCommonConstants.DATABASE_ID,
                MLModelCommonConstants.MODEL_NAME,
                MLModelCommonConstants.MODEL_VERSION,
                MLModelCommonConstants.KMS_SECRET_RETRY_COUNT,
                MLModelCommonConstants.KMS_SECRET_RETRY_DELAY_MS
            })
    public void testConfiguration(String param) throws IllegalAccessException {
        model = modelFromOption(options);
        Map<String, String> config = new HashMap<>();
        if (isNumberConfig(param)) {
            config.put(param, "200");
        } else {
            config.put(param, "local");
        }

        decrypter = new ModelSecretDecrypter(model, config);
        ParsedConfig parsedConfig = decrypter.getParsedConfig();
        Field[] fields = parsedConfig.getClass().getFields();
        for (Field field : fields) {
            String configName = field.getAnnotation(ConfigAnnotation.class).name();
            if (param.equals(configName)) {
                if (isNumberConfig(param)) {
                    assertThat(field.get(parsedConfig)).isEqualTo(200);
                } else {
                    assertThat(field.get(parsedConfig)).isEqualTo("local");
                }
            } else {
                // Not override by config, get from model
                if (configName.equals(MLModelCommonConstants.KMS_SECRET_RETRY_DELAY_MS)) {
                    assertThat(field.get(parsedConfig))
                            .isEqualTo(FlinkCredentialServiceSecretDecrypter.DELAY_MS_DEFAULT);
                } else if (configName.equals(MLModelCommonConstants.KMS_SECRET_RETRY_COUNT)) {
                    assertThat(field.get(parsedConfig))
                            .isEqualTo(FlinkCredentialServiceSecretDecrypter.MAX_ATTEMPT_DEFAULT);
                } else if (configName.equals(MLModelCommonConstants.CREDENTIAL_SERVICE_PORT)) {
                    assertThat(field.get(parsedConfig)).isEqualTo(123);
                } else {
                    assertThat(field.get(parsedConfig)).isEqualTo(options.get(configName));
                }
            }
        }
    }

    private static CatalogModel modelFromOption(Map<String, String> options) {
        return CatalogModel.of(
                Schema.newBuilder().build(), Schema.newBuilder().build(), options, null);
    }

    private static boolean isNumberConfig(String key) {
        return key.equals(MLModelCommonConstants.KMS_SECRET_RETRY_COUNT)
                || key.equals(MLModelCommonConstants.KMS_SECRET_RETRY_DELAY_MS)
                || key.equals(MLModelCommonConstants.CREDENTIAL_SERVICE_PORT);
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
