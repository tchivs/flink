/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.util.FlinkRuntimeException;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentials;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialsRequest;
import cloud.confluent.ksql_api_service.flinkcredential.GetCredentialsResponse;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

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
        GetCredentialsResponse response;
        try {
            GetCredentialsRequest request =
                    GetCredentialsRequest.newBuilder()
                            .setComputePoolId(jobCredentialsMetadata.getComputePoolId())
                            .setIdentityPoolId(jobCredentialsMetadata.getIdentityPoolId())
                            .build();
            response = credentialService.getCredentials(request);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to do credential request for compute pool %s, identity pool %s",
                            jobCredentialsMetadata.getComputePoolId(),
                            jobCredentialsMetadata.getIdentityPoolId()),
                    t);
        }
        FlinkCredentials flinkCredentials = response.getFlinkCredentials();
        String apiKey = flinkCredentials.getApiKey();
        String secret =
                new String(
                        decrypter.decrypt(flinkCredentials.getEncryptedSecret().toByteArray()),
                        StandardCharsets.UTF_8);
        return Pair.of(apiKey, secret);
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
}
