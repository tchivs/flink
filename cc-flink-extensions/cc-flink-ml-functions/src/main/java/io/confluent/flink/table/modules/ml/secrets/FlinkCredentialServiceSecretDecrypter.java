/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.secrets;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogModel;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import cloud.confluent.ksql_api_service.flinkcredential.KMSDecryptionRequest;
import cloud.confluent.ksql_api_service.flinkcredential.KMSDecryptionResponse;
import com.google.protobuf.ByteString;
import io.confluent.flink.credentials.utils.CallWithRetry;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;
import io.confluent.flink.table.utils.mlutils.MlUtils;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import java.util.Objects;
import java.util.function.Function;

/** Secret decrypter from Flink credential service. */
public class FlinkCredentialServiceSecretDecrypter implements SecretDecrypter {
    // TODO (MATRIX-127) get from config
    private static final int MAX_ATTEMPT = 3;
    private static final int DELAY_MS = 1000;

    private final ModelOptionsUtils modelOptionsUtils;
    private final FlinkCredentialServiceBlockingStub credentialService;
    private final Function<String, byte[]> dataSigner;
    private final CallWithRetry callWithRetry;

    public FlinkCredentialServiceSecretDecrypter(CatalogModel model) {
        modelOptionsUtils = new ModelOptionsUtils(model.getOptions());
        Channel channel =
                ManagedChannelBuilder.forAddress(
                                modelOptionsUtils.getCredentialServiceHost(),
                                modelOptionsUtils.getCredentialServicePort())
                        .usePlaintext()
                        .build();
        credentialService = FlinkCredentialServiceGrpc.newBlockingStub(channel);
        dataSigner = MlUtils::signData;
        callWithRetry = new CallWithRetry(MAX_ATTEMPT, DELAY_MS);
    }

    @VisibleForTesting
    public FlinkCredentialServiceSecretDecrypter(
            CatalogModel model,
            FlinkCredentialServiceBlockingStub stub,
            Function<String, byte[]> dataSigner,
            CallWithRetry callWithRetry) {
        modelOptionsUtils = new ModelOptionsUtils(model.getOptions());
        credentialService = stub;
        this.dataSigner = dataSigner;
        this.callWithRetry = callWithRetry;
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

        // OrgId, compute pool id, compute pool env are required
        final String orgId =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.ORG_ID),
                        MLModelCommonConstants.ORG_ID);
        final String computePoolId =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.COMPUTE_POOL_ID),
                        MLModelCommonConstants.COMPUTE_POOL_ID);
        final String computePoolEnvId =
                Objects.requireNonNull(
                        modelOptionsUtils.getOption(MLModelCommonConstants.COMPUTE_POOL_ENV_ID),
                        MLModelCommonConstants.COMPUTE_POOL_ENV_ID);

        // Below are optional fields
        final String envId =
                modelOptionsUtils.getOptionOrDefault(MLModelCommonConstants.ENV_ID, "");
        final String dbId =
                modelOptionsUtils.getOptionOrDefault(MLModelCommonConstants.DATABASE_ID, "");
        final String modelName =
                modelOptionsUtils.getOptionOrDefault(MLModelCommonConstants.MODEL_NAME, "");
        final String modelVersion =
                modelOptionsUtils.getOptionOrDefault(MLModelCommonConstants.MODEL_VERSION, "");

        final byte[] signature = dataSigner.apply(secret);
        final KMSDecryptionRequest.Builder builder =
                KMSDecryptionRequest.newBuilder()
                        .setCipherText(secret)
                        .setSignature(ByteString.copyFrom(signature))
                        .setOrgId(orgId)
                        .setEnvironmentId(envId)
                        .setDatabaseId(dbId)
                        .setComputePoolId(computePoolId)
                        .setComputePoolEnvId(computePoolEnvId)
                        .setResourceName(modelName)
                        .setResourceVersion(modelVersion);

        final KMSDecryptionResponse response =
                callWithRetry.call(() -> credentialService.decryptSecret(builder.build()));
        return response.getPlainText().toStringUtf8();
    }

    @Override
    public EncryptionStrategy supportedStrategy() {
        return EncryptionStrategy.KMS;
    }

    @Override
    public MLModelSupportedProviders getProvider() {
        return MLModelSupportedProviders.fromString(modelOptionsUtils.getProvider());
    }
}
