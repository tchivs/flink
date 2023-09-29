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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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

    private final Map<JobID, JobTriple> credentialsByJobID = new ConcurrentHashMap<>();

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
        credentialsByJobID.putIfAbsent(jobId, JobTriple.onRegisterJob(jobConfiguration));
        return true;
    }

    @Override
    public void unregisterJob(JobID jobId) {
        LOG.info("Unregistering job {}", jobId.toHexString());
        credentialsByJobID.remove(jobId);
    }

    @Override
    public boolean delegationTokensRequired() {
        return enabled;
    }

    private void fetchToken(JobCredentialsMetadata jobCredentialsMetadata) {
        try {
            KafkaCredentials credentials =
                    kafkaCredentialFetcher.fetchToken(jobCredentialsMetadata);
            credentialsByJobID.compute(
                    jobCredentialsMetadata.getJobID(),
                    (k, existingTriple) -> {
                        if (existingTriple == null) {
                            LOG.warn(
                                    "Job {} was stopped before finishing fetching credentials.",
                                    k.toHexString());
                            return null;
                        }
                        return JobTriple.onCredentialsFetched(
                                existingTriple.getJobConfiguration(),
                                jobCredentialsMetadata,
                                credentials);
                    });
        } catch (Throwable t) {
            LOG.error("Couldn't fetch kafka token", t);
        }
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        final Set<JobID> jobIDs = new HashSet<>(credentialsByJobID.keySet());
        // Find the jobs which are new, so we can create the metadata object used for fetching.
        for (JobID jobID : jobIDs) {
            JobTriple newJob = credentialsByJobID.get(jobID);
            if (newJob == null || newJob.credentialsExist()) {
                continue;
            }
            LOG.info("Fetching DPAT for new job {}", jobID.toHexString());
            Configuration jobConfiguration = newJob.getJobConfiguration();

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
        // Find the jobs which are in need of updated credentials.
        for (JobID jobID : jobIDs) {
            JobTriple updateJob = credentialsByJobID.get(jobID);
            if (updateJob == null || !updateJob.credentialsExist()) {
                continue;
            }
            JobCredentialsMetadata metadata = updateJob.getJobCredentialsMetadata();
            if (clock.absoluteTimeMillis() - metadata.getTokenUpdateTimeMs() > expirationMs) {
                LOG.info("Updating DPAT for job {}", metadata.getJobID().toHexString());
                JobCredentialsMetadata jobCredentialsMetadata =
                        metadata.withNewTokenUpdateTime(clock.absoluteTimeMillis());
                fetchToken(jobCredentialsMetadata);
            }
        }

        // Simplify the data we cache
        Map<JobID, KafkaCredentials> credentials =
                credentialsByJobID.entrySet().stream()
                        .filter(e -> e.getValue().credentialsExist())
                        .collect(
                                Collectors.toMap(
                                        Entry::getKey, e -> e.getValue().getKafkaCredentials()));

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

    /**
     * Utility class to manage the data we're fetching. This class is immutable.
     *
     * <p>These objects come in two states, right after the job has been registered, and after
     * credentials have been fetched at least once.
     */
    private static class JobTriple {
        private final Configuration jobConfiguration;
        private final JobCredentialsMetadata jobCredentialsMetadata;
        private final KafkaCredentials kafkaCredentials;

        /**
         * Creates a triple with the configuration set, but not other fields. This indicates that
         * it's the first time we're going to fetch the credentials.
         *
         * @param jobConfiguration The job configuration
         * @return The triple
         */
        public static JobTriple onRegisterJob(Configuration jobConfiguration) {
            return new JobTriple(jobConfiguration, null, null);
        }

        /**
         * Creates a triple with all fields set, indicating that the job has successfully fetched
         * credentials and is ready to cache them.
         *
         * @param jobConfiguration The job configuration
         * @param jobCredentialsMetadata The job credential metadata
         * @param kafkaCredentials The fetched credentials
         * @return The triple
         */
        public static JobTriple onCredentialsFetched(
                Configuration jobConfiguration,
                JobCredentialsMetadata jobCredentialsMetadata,
                KafkaCredentials kafkaCredentials) {
            return new JobTriple(jobConfiguration, jobCredentialsMetadata, kafkaCredentials);
        }

        public JobTriple(
                Configuration jobConfiguration,
                JobCredentialsMetadata jobCredentialsMetadata,
                KafkaCredentials kafkaCredentials) {
            this.jobConfiguration = jobConfiguration;
            this.jobCredentialsMetadata = jobCredentialsMetadata;
            this.kafkaCredentials = kafkaCredentials;
        }

        public Configuration getJobConfiguration() {
            return jobConfiguration;
        }

        public JobCredentialsMetadata getJobCredentialsMetadata() {
            return jobCredentialsMetadata;
        }

        public KafkaCredentials getKafkaCredentials() {
            return kafkaCredentials;
        }

        public boolean credentialsExist() {
            return jobCredentialsMetadata != null && kafkaCredentials != null;
        }
    }
}
