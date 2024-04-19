/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogModel;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import cloud.confluent.ksql_api_service.flinkcredential.KMSDecryptionRequest;
import cloud.confluent.ksql_api_service.flinkcredential.KMSDecryptionResponse;
import cloud.confluent.ksql_api_service.flinkcredential.KeyMeta;
import com.google.protobuf.ByteString;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.confluent.flink.table.utils.MlUtils;
import io.confluent.flink.table.utils.ModelOptionsUtils;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import java.util.Base64;
import java.util.Objects;

/** Secret decrypter from Flink credential service. */
public class FlinkCredentialServiceSecretDecrypter implements SecretDecrypter {
    private final ModelOptionsUtils modelOptionsUtils;
    private final FlinkCredentialServiceBlockingStub credentialService;

    public FlinkCredentialServiceSecretDecrypter(CatalogModel model) {
        modelOptionsUtils = new ModelOptionsUtils(model.getOptions());
        Channel channel =
                ManagedChannelBuilder.forAddress(
                                modelOptionsUtils.getCredentialServiceHost(),
                                modelOptionsUtils.getCredentialServicePort())
                        .usePlaintext()
                        .build();
        credentialService = FlinkCredentialServiceGrpc.newBlockingStub(channel);
    }

    @VisibleForTesting
    public FlinkCredentialServiceSecretDecrypter(
            CatalogModel model, FlinkCredentialServiceBlockingStub stub) {
        modelOptionsUtils = new ModelOptionsUtils(model.getOptions());
        credentialService = stub;
    }

    @Override
    public String decryptFromKey(final String secretKey) {
        final String secret = modelOptionsUtils.getOption(secretKey);
        if (secret == null || secret.isEmpty()) {
            return "";
        }

        final String strategy = modelOptionsUtils.getEncryptStrategy();
        if (!EncryptionStrategy.KMS.name().equalsIgnoreCase(strategy)) {
            // Caller's responsibility to ensure decrypting right key
            throw new IllegalArgumentException("Decrypt key which is not KMS encrypted");
        }

        final String orgId =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.ORG_ID),
                        MLModelCommonConstants.ORG_ID);
        final String envId =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.ENV_ID),
                        MLModelCommonConstants.ENV_ID);
        final String dbId =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.DATABASE_ID),
                        MLModelCommonConstants.DATABASE_ID);
        final String computePoolId =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.COMPUTE_POOL_ID),
                        MLModelCommonConstants.COMPUTE_POOL_ID);
        final String modelName =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.MODEL_NAME),
                        MLModelCommonConstants.MODEL_NAME);
        final String modelVersion =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.MODEL_VERSION),
                        MLModelCommonConstants.MODEL_VERSION);
        final String keyVersionKey =
                MLModelCommonConstants.MODEL_KMS_KEY_VERSION_PREFIX + "." + secretKey;
        final String keyVersion = modelOptionsUtils.getOption(keyVersionKey);

        // Secret is base64 encoded for kms. Decoded it first
        final byte[] decodedSecret = Base64.getDecoder().decode(secret);

        final KMSDecryptionRequest.Builder builder =
                KMSDecryptionRequest.newBuilder()
                        .setCipherText(ByteString.copyFrom(decodedSecret))
                        .setOrgId(orgId)
                        .setEnvironmentId(envId)
                        .setDatabaseId(dbId)
                        .setComputePoolId(computePoolId)
                        .setResourceName(modelName)
                        .setResourceVersion(modelVersion);
        if (keyVersion != null) {
            builder.setKeyMeta(KeyMeta.newBuilder().setKeyVersion(keyVersion).build());
        }

        // TODO (matrix-96): check status code and retry
        final KMSDecryptionResponse response = credentialService.decryptSecret(builder.build());
        return MlUtils.decryptWithComputePoolSecret(response.getPlainText().toByteArray());
    }

    @Override
    public RemoteModelOptions.EncryptionStrategy supportedStrategy() {
        return EncryptionStrategy.KMS;
    }
}
