/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc;
import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.confluent.flink.credentials.JobOptions.COMMA_SEPARATED_PRINCIPALS;
import static io.confluent.flink.credentials.JobOptions.COMPUTE_POOL_ID;
import static io.confluent.flink.credentials.JobOptions.IDENTITY_POOL_ID;
import static io.confluent.flink.credentials.JobOptions.STATEMENT_ID_CRN;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.AUTH_SERVICE_SERVER;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_CHECK_PERIOD_MS;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_EXPIRATION_MS;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_HOST;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_PORT;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.DPAT_ENABLED;

/**
 * This class fetches Kafka tokens for a cluster.
 *
 * <p>In more detail, {@link KafkaDelegationTokenProvider} is created on the JM and {@link
 * DelegationTokenProvider#obtainDelegationTokens()} is called periodically, fetching a map of
 * tokens, serializing them, and doing an rpc to each TM. The TMs all instantiate an instance of
 * {@link KafkaDelegationTokenReceiver} on startup, and upon getting the rpc, pass the serialized
 * token to the receiver. This allows this class to do the fetching of tokens in a single place and
 * then fans out the tokens to all the TMs.
 */
@Confluent
public class KafkaDelegationTokenProvider implements DelegationTokenProvider {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaDelegationTokenProvider.class);
    private final Map<JobID, Pair<JobCredentialsMetadata, KafkaCredentials>> credentialsByJobID =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<JobID, Configuration> jobsToFetch = new ConcurrentHashMap<>();

    private final CredentialDecrypter credentialDecrypter;

    // These are initialized right after creation
    private KafkaCredentialFetcher kafkaCredentialFetcher;
    private boolean enabled;
    private long expirationMs;
    private long checkPeriodMs;
    private Clock clock;

    public KafkaDelegationTokenProvider() {
        this.credentialDecrypter = CredentialDecrypterImpl.INSTANCE;
    }

    @VisibleForTesting
    KafkaDelegationTokenProvider(
            KafkaCredentialFetcher kafkaCredentialFetcher,
            CredentialDecrypter credentialDecrypter,
            long expirationMs,
            long checkPeriodMs,
            Clock clock,
            boolean enabled) {
        this.kafkaCredentialFetcher = kafkaCredentialFetcher;
        this.credentialDecrypter = credentialDecrypter;
        this.expirationMs = expirationMs;
        this.checkPeriodMs = checkPeriodMs;
        this.clock = clock;
        this.enabled = enabled;
    }

    @Override
    public String serviceName() {
        return "Kafka";
    }

    @Override
    public void init(Configuration configuration) {
        enabled = configuration.getBoolean(DPAT_ENABLED);
        if (!enabled) {
            LOG.info("DPAT fetching is DISABLED.");
            return;
        }
        credentialDecrypter.init(configuration);
        Channel channel =
                ManagedChannelBuilder.forAddress(
                                configuration.getString(CREDENTIAL_SERVICE_HOST),
                                configuration.getInteger(CREDENTIAL_SERVICE_PORT))
                        .usePlaintext()
                        .build();
        FlinkCredentialServiceBlockingStub credentialService =
                FlinkCredentialServiceGrpc.newBlockingStub(channel);
        TokenExchangerImpl tokenExchanger =
                new TokenExchangerImpl(configuration.getString(AUTH_SERVICE_SERVER));
        kafkaCredentialFetcher =
                new KafkaCredentialFetcherImpl(
                        credentialService, tokenExchanger, credentialDecrypter);
        expirationMs = configuration.getLong(CREDENTIAL_EXPIRATION_MS);
        checkPeriodMs = configuration.getLong(CREDENTIAL_CHECK_PERIOD_MS);
        LOG.info(
                "DPAT fetching is enabled with expirationMs {}, checkPeriodMs {}.",
                expirationMs,
                checkPeriodMs);
        clock = SystemClock.getInstance();
    }

    @Override
    public boolean registerJob(JobID jobId, Configuration jobConfiguration) {
        LOG.info("Registering new job {}", jobId.toHexString());
        jobsToFetch.put(jobId, jobConfiguration);
        return true;
    }

    @Override
    public void unregisterJob(JobID jobId) {
        LOG.info("Unregistering job {}", jobId.toHexString());
        credentialsByJobID.remove(jobId);
        jobsToFetch.remove(jobId);
    }

    @Override
    public boolean delegationTokensRequired() {
        return enabled;
    }

    private void fetchToken(JobCredentialsMetadata jobCredentialsMetadata) {
        try {
            KafkaCredentials credentials =
                    kafkaCredentialFetcher.fetchToken(jobCredentialsMetadata);
            credentialsByJobID.put(
                    jobCredentialsMetadata.getJobID(),
                    Pair.of(jobCredentialsMetadata, credentials));
        } catch (Throwable t) {
            LOG.error("Couldn't fetch kafka token", t);
        }
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        Set<JobID> jobIds = jobsToFetch.keySet();
        for (JobID jobID : jobIds) {
            LOG.info("Fetching DPAT for new job {}", jobID.toHexString());
            Configuration jobConfiguration = jobsToFetch.remove(jobID);
            if (jobConfiguration == null) {
                continue;
            }

            List<String> principals = parsePrincipals(jobConfiguration, jobID.toHexString());

            JobCredentialsMetadata jobCredentialsMetadata =
                    new JobCredentialsMetadata(
                            jobID,
                            jobConfiguration.getString(STATEMENT_ID_CRN),
                            jobConfiguration.getString(COMPUTE_POOL_ID),
                            jobConfiguration.getString(IDENTITY_POOL_ID),
                            principals,
                            clock.absoluteTimeMillis(),
                            clock.absoluteTimeMillis());
            fetchToken(jobCredentialsMetadata);
        }
        for (Pair<JobCredentialsMetadata, KafkaCredentials> p : credentialsByJobID.values()) {
            if (clock.absoluteTimeMillis() - p.getKey().getTokenUpdateTimeMs() > expirationMs) {
                LOG.info("Updating DPAT for job {}", p.getKey().getJobID().toHexString());
                JobCredentialsMetadata jobCredentialsMetadata =
                        p.getKey().withNewTokenUpdateTime(clock.absoluteTimeMillis());
                fetchToken(jobCredentialsMetadata);
            }
        }

        // Simplify the data we cache
        Map<JobID, KafkaCredentials> credentials =
                credentialsByJobID.entrySet().stream()
                        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().getValue()));

        LOG.info("Sending credentials for jobs {}", credentials.keySet());
        return new ObtainedDelegationTokens(
                InstantiationUtil.serializeObject(credentials),
                Optional.of(clock.absoluteTimeMillis() + checkPeriodMs));
    }

    private static List<String> parsePrincipals(Configuration jobConfiguration, String jobId) {
        String commaSeparatedPrincipals = jobConfiguration.getString(COMMA_SEPARATED_PRINCIPALS);
        List<String> principals =
                commaSeparatedPrincipals == null
                        ? Collections.emptyList()
                        : Arrays.stream(commaSeparatedPrincipals.split(","))
                                .collect(Collectors.toList());

        LOG.info("Principals {} for jobId {}", principals, jobId);
        return principals;
    }
}
