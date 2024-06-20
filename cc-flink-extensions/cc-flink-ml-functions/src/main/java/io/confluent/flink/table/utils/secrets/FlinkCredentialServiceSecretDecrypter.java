/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.secrets;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import cloud.confluent.ksql_api_service.flinkcredential.KMSDecryptionRequest;
import cloud.confluent.ksql_api_service.flinkcredential.KMSDecryptionResponse;
import com.google.protobuf.ByteString;
import io.confluent.flink.credentials.utils.CallWithRetry;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.confluent.flink.table.utils.RemoteRuntimeUtils;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.function.Function;

/** Secret decrypter from Flink credential service. */
public abstract class FlinkCredentialServiceSecretDecrypter<T> implements SecretDecrypter {

    /** Config annotation for ParsedConfig fields. */
    @Target({ElementType.TYPE, ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    @Public
    public @interface ConfigAnnotation {
        String name();
    }

    /** Parsed configuration pojo. */
    public static class ParsedConfig {
        // Below are required fields
        @ConfigAnnotation(name = MLModelCommonConstants.ORG_ID)
        public String orgId;

        @ConfigAnnotation(name = MLModelCommonConstants.COMPUTE_POOL_ID)
        public String computePoolId;

        @ConfigAnnotation(name = MLModelCommonConstants.COMPUTE_POOL_ENV_ID)
        public String computePoolEnvId;

        @ConfigAnnotation(name = MLModelCommonConstants.CREDENTIAL_SERVICE_HOST)
        public String credentialServiceHost;

        @ConfigAnnotation(name = MLModelCommonConstants.CREDENTIAL_SERVICE_PORT)
        public int credentialServicePort;

        @ConfigAnnotation(name = MLModelCommonConstants.ENCRYPT_STRATEGY)
        public String encryptStrategy;

        // Below are optional fields
        @ConfigAnnotation(name = MLModelCommonConstants.DATABASE_ID)
        public String databaseId;

        @ConfigAnnotation(name = MLModelCommonConstants.ENV_ID)
        public String resourceEnvId;

        @ConfigAnnotation(name = MLModelCommonConstants.MODEL_VERSION)
        public String resourceVersion;

        @ConfigAnnotation(name = MLModelCommonConstants.MODEL_NAME)
        public String resourceName;

        @ConfigAnnotation(name = MLModelCommonConstants.KMS_SECRET_RETRY_COUNT)
        public int maxAttempt;

        @ConfigAnnotation(name = MLModelCommonConstants.KMS_SECRET_RETRY_DELAY_MS)
        public int delayMs;
    }

    // TODO (MATRIX-127) get from config
    public static final int MAX_ATTEMPT_DEFAULT = 3;
    public static final int DELAY_MS_DEFAULT = 1000;

    private final FlinkCredentialServiceBlockingStub credentialService;
    private final Function<String, byte[]> dataSigner;
    private final CallWithRetry callWithRetry;
    protected final ParsedConfig parsedConfig;
    protected final T resource;
    protected final Map<String, String> configuration;

    public FlinkCredentialServiceSecretDecrypter(T resource, Map<String, String> configuration) {
        this.configuration = configuration;
        this.resource = resource;
        parsedConfig = configure(resource, configuration);
        Channel channel =
                ManagedChannelBuilder.forAddress(
                                parsedConfig.credentialServiceHost,
                                parsedConfig.credentialServicePort)
                        .usePlaintext()
                        .build();
        credentialService = FlinkCredentialServiceGrpc.newBlockingStub(channel);
        dataSigner = RemoteRuntimeUtils::signData;
        callWithRetry = new CallWithRetry(parsedConfig.maxAttempt, parsedConfig.delayMs);
    }

    @VisibleForTesting
    public FlinkCredentialServiceSecretDecrypter(
            T resource,
            Map<String, String> configuration,
            FlinkCredentialServiceBlockingStub stub,
            Function<String, byte[]> dataSigner,
            CallWithRetry callWithRetry) {
        this.resource = resource;
        this.configuration = configuration;
        parsedConfig = configure(resource, configuration);
        credentialService = stub;
        this.dataSigner = dataSigner;
        this.callWithRetry = callWithRetry;
    }

    @Override
    public String decryptFromKey(final String secretKey) {
        final String secret = getSecretValue(secretKey);
        if (secret == null || secret.isEmpty()) {
            return "";
        }

        final String strategy = parsedConfig.encryptStrategy;
        if (!EncryptionStrategy.KMS.name().equalsIgnoreCase(strategy)) {
            // Caller's responsibility to ensure decrypting right key
            throw new IllegalArgumentException("Decrypt key which is not KMS encrypted");
        }

        final byte[] signature = dataSigner.apply(secret);
        final KMSDecryptionRequest.Builder builder =
                KMSDecryptionRequest.newBuilder()
                        .setCipherText(secret)
                        .setSignature(ByteString.copyFrom(signature))
                        .setOrgId(parsedConfig.orgId)
                        .setEnvironmentId(parsedConfig.resourceEnvId)
                        .setDatabaseId(parsedConfig.databaseId)
                        .setComputePoolId(parsedConfig.computePoolId)
                        .setComputePoolEnvId(parsedConfig.computePoolEnvId)
                        .setResourceName(parsedConfig.resourceName)
                        .setResourceVersion(parsedConfig.resourceVersion);

        final KMSDecryptionResponse response =
                callWithRetry.call(() -> credentialService.decryptSecret(builder.build()));
        return response.getPlainText().toStringUtf8();
    }

    abstract ParsedConfig configure(T resource, Map<String, String> configuration);

    @Override
    public EncryptionStrategy supportedStrategy() {
        return EncryptionStrategy.KMS;
    }

    abstract String getSecretValue(String secretKey);

    ParsedConfig getParsedConfig() {
        return parsedConfig;
    }
}
