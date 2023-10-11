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
import io.grpc.CallOptions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    private Triple<String, String, String> extractOrgAndEnvFromCRN(String crn) {
        Map<String, String> parts =
                Arrays.stream(crn.split("/"))
                        .filter(
                                s ->
                                        s.startsWith("organization=")
                                                || s.startsWith("environment=")
                                                || s.startsWith("statement="))
                        .map(
                                s -> {
                                    int delimiterIdx = s.indexOf("=");
                                    return Pair.of(
                                            s.substring(0, delimiterIdx),
                                            s.substring(delimiterIdx + 1));
                                })
                        .collect(
                                Collectors.groupingBy(
                                        p -> p.getLeft(),
                                        Collectors.collectingAndThen(
                                                Collectors.toList(),
                                                l -> {
                                                    if (l.size() != 1) {
                                                        throw new FlinkRuntimeException(
                                                                String.format(
                                                                        "Failed to extract org and env from CRN: %s, found multiple values for: %s",
                                                                        crn, l.get(0).getLeft()));
                                                    }
                                                    return l.get(0).getRight();
                                                })));

        if (parts.size() != 3
                || !parts.containsKey("organization")
                || !parts.containsKey("environment")
                || !parts.containsKey("statement")) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to extract org and env from CRN, one of 'organization', 'environment', 'statement' missing in %s",
                            crn));
        }

        return Triple.of(
                parts.get("organization"), parts.get("environment"), parts.get("statement"));
    }

    // Fetch the single static credential associated to the computePool. Principals are provided
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

        final CallOptions.Key<Object> orgIdKey = CallOptions.Key.create("org_resource_id");
        final CallOptions.Key<Object> envIdKey = CallOptions.Key.create("environment_id");
        final CallOptions.Key<Object> statementIdKey = CallOptions.Key.create("statement_id");
        final CallOptions.Key<Object> crnKey = CallOptions.Key.create("crn");
        final Triple<String, String, String> orgIdAndEnv =
                extractOrgAndEnvFromCRN(jobCredentialsMetadata.getStatementIdCRN());

        response =
                credentialService
                        .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                        .withOption(orgIdKey, orgIdAndEnv.getLeft())
                        .withOption(envIdKey, orgIdAndEnv.getMiddle())
                        .withOption(statementIdKey, orgIdAndEnv.getRight())
                        .withOption(crnKey, jobCredentialsMetadata.getStatementIdCRN())
                        .getCredentialV2(request);
        apiKey = response.getFlinkCredentials().getApiKey();
        encryptedSecret = response.getFlinkCredentials().getEncryptedSecret();
        LOG.info(
                "Successfully fetched v2 static credential {}, {}", apiKey, jobCredentialsMetadata);
        return Pair.of(apiKey, encryptedSecret);
    }
}
