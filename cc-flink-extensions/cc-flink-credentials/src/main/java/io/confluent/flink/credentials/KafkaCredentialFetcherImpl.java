/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.util.FlinkRuntimeException;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialRequestV2;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialResponseV2;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialsRequest;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialsResponse;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static io.confluent.flink.credentials.TokenExchangerImpl.isLegacyIdentityPoolFlow;

/**
 * This class does the token exchange process to fetch a DPAT token used when accessing Kafka,
 * Schema Registry and other services.
 */
@Confluent
public class KafkaCredentialFetcherImpl implements KafkaCredentialFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCredentialFetcherImpl.class);

    private final FlinkCredentialServiceBlockingStub credentialService;
    private final TokenExchanger tokenExchanger;
    private final CredentialDecrypter decrypter;

    public KafkaCredentialFetcherImpl(
            FlinkCredentialServiceBlockingStub credentialService,
            TokenExchanger tokenExchanger,
            CredentialDecrypter decrypter) {
        this.credentialService = credentialService;
        this.tokenExchanger = tokenExchanger;
        this.decrypter = decrypter;
    }

    @Override
    public KafkaCredentials fetchToken(JobCredentialsMetadata jobCredentialsMetadata) {
        Pair<String, String> ccAuthCredentials =
                fetchStaticCredentialsForJob(jobCredentialsMetadata);
        DPATToken dpat = exchangeForToken(ccAuthCredentials, jobCredentialsMetadata);
        return new KafkaCredentials(dpat.getToken());
    }

    private Pair<String, String> fetchStaticCredentialsForJob(
            JobCredentialsMetadata jobCredentialsMetadata) {

        Pair<String, ByteString> apiKeyAndEncryptedSecret;
        try {
            apiKeyAndEncryptedSecret = fetchKeyAndEncryptedSecret(jobCredentialsMetadata);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to do credential request for JobCredentials %s",
                            jobCredentialsMetadata),
                    t);
        }
        String secret =
                new String(
                        decrypter.decrypt(apiKeyAndEncryptedSecret.getRight().toByteArray()),
                        StandardCharsets.UTF_8);
        return Pair.of(apiKeyAndEncryptedSecret.getLeft(), secret);
    }

    private DPATToken exchangeForToken(
            Pair<String, String> ccAuthCredentials, JobCredentialsMetadata jobCredentialsMetadata) {
        try {
            return tokenExchanger.fetch(ccAuthCredentials, jobCredentialsMetadata);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to do fetch DPAT Token for compute pool %s, identity pool %s",
                            jobCredentialsMetadata.getComputePoolId(),
                            jobCredentialsMetadata.getIdentityPoolId()),
                    t);
        }
    }

    private Pair<String, ByteString> fetchKeyAndEncryptedSecret(
            JobCredentialsMetadata jobCredentialsMetadata) {

        if (jobCredentialsMetadata == null) {
            throw new FlinkRuntimeException(
                    "JobCredentialMetadata is empty, can't fetch credentials");
        }

        if (isLegacyIdentityPoolFlow(jobCredentialsMetadata)) {
            return fetchCredential(jobCredentialsMetadata);
        } else {
            return fetchCredentialV2(jobCredentialsMetadata);
        }
    }

    // Fetch credential for the Pair<ComputePool, IdentityPool>, will be deprecated soon
    private Pair<String, ByteString> fetchCredential(
            JobCredentialsMetadata jobCredentialsMetadata) {
        ByteString encryptedSecret;
        String apiKey;
        GetCredentialsResponse response;
        GetCredentialsRequest request =
                GetCredentialsRequest.newBuilder()
                        .setComputePoolId(jobCredentialsMetadata.getComputePoolId())
                        .setIdentityPoolId(jobCredentialsMetadata.getIdentityPoolId())
                        .build();
        response = credentialService.getCredentials(request);
        apiKey = response.getFlinkCredentials().getApiKey();
        encryptedSecret = response.getFlinkCredentials().getEncryptedSecret();
        LOG.info(
                "Successfully fetched v2 static credential {}, {}", apiKey, jobCredentialsMetadata);
        return Pair.of(apiKey, encryptedSecret);
    }

    // Fetch the single static credential associated to the computePool. Principals are provided
    // for audit logging but there is only one single key for each compute pool.
    private Pair<String, ByteString> fetchCredentialV2(
            JobCredentialsMetadata jobCredentialsMetadata) {
        ByteString encryptedSecret;
        String apiKey;
        GetCredentialResponseV2 response;
        final GetCredentialRequestV2 request =
                GetCredentialRequestV2.newBuilder()
                        .setComputePoolId(jobCredentialsMetadata.getComputePoolId())
                        .addAllPrincipalIds(jobCredentialsMetadata.getPrincipals())
                        .build();
        response = credentialService.getCredentialV2(request);
        apiKey = response.getFlinkCredentials().getApiKey();
        encryptedSecret = response.getFlinkCredentials().getEncryptedSecret();
        LOG.info(
                "Successfully fetched v1 static credential {}, {}", apiKey, jobCredentialsMetadata);
        return Pair.of(apiKey, encryptedSecret);
    }
}
