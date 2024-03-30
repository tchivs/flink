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
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

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
    private final long deadlineMs;

    public KafkaCredentialFetcherImpl(
            FlinkCredentialServiceBlockingStub credentialService,
            TokenExchanger tokenExchanger,
            CredentialDecrypter decrypter,
            long deadlineMs) {
        this.credentialService = credentialService;
        this.tokenExchanger = tokenExchanger;
        this.decrypter = decrypter;
        this.deadlineMs = deadlineMs;
    }

    @Override
    public KafkaCredentials fetchToken(JobCredentialsMetadata jobCredentialsMetadata) {
        Pair<String, String> ccAuthCredentials =
                fetchStaticCredentialsForJob(jobCredentialsMetadata);
        DPATTokens dpats = exchangeForToken(ccAuthCredentials, jobCredentialsMetadata);
        return new KafkaCredentials(dpats.getToken(), dpats.getUDFToken());
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

    private DPATTokens exchangeForToken(
            Pair<String, String> ccAuthCredentials, JobCredentialsMetadata jobCredentialsMetadata) {
        try {
            return tokenExchanger.fetch(ccAuthCredentials, jobCredentialsMetadata);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    String.format("Failed to do fetch DPAT Token: %s", jobCredentialsMetadata), t);
        }
    }

    private Pair<String, ByteString> fetchKeyAndEncryptedSecret(
            JobCredentialsMetadata jobCredentialsMetadata) {

        if (jobCredentialsMetadata == null) {
            throw new FlinkRuntimeException(
                    "JobCredentialMetadata is empty, can't fetch credentials");
        }

        LOG.info("Fetching credential for job metadata {}", jobCredentialsMetadata);
        return fetchCredentialV2(jobCredentialsMetadata);
    }

    // Fetch the single static credential associated to the computePool. Principals
    // are provided
    // for audit logging but there is only one single key for each compute pool.
    private Pair<String, ByteString> fetchCredentialV2(
            JobCredentialsMetadata jobCredentialsMetadata) {
        LOG.info("Fetching credential for v2 (sa or user). {}", jobCredentialsMetadata);
        ByteString encryptedSecret;
        String apiKey;
        GetCredentialResponseV2 response;
        final GetCredentialRequestV2 request =
                GetCredentialRequestV2.newBuilder()
                        .setComputePoolId(jobCredentialsMetadata.getComputePoolId())
                        .addAllPrincipalIds(jobCredentialsMetadata.getPrincipals())
                        .build();

        final Metadata md = new Metadata();
        md.put(
                Metadata.Key.of("crn", Metadata.ASCII_STRING_MARSHALLER),
                jobCredentialsMetadata.getStatementIdCRN());

        response =
                credentialService
                        .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
                        .getCredentialV2(request);
        apiKey = response.getFlinkCredentials().getApiKey();
        encryptedSecret = response.getFlinkCredentials().getEncryptedSecret();
        LOG.info(
                "Successfully fetched v2 static credential {}, {}", apiKey, jobCredentialsMetadata);
        return Pair.of(apiKey, encryptedSecret);
    }
}
