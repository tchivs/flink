/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token.kafka;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Used to cache credentials for Kafka and Schema Registry. There must be a periodic service which
 * fetches and them sets the credentials on this cache with {@code onNewCredentialsObtained}. The
 * cache allows calls to {@code getCredentials} enabling callers to wait until they arrive.
 */
@Confluent
public class KafkaCredentialsCacheImpl implements KafkaCredentialsCache {

    @GuardedBy("this")
    private Map<JobID, KafkaCredentials> credentialsByJobId = new HashMap<>();

    @GuardedBy("this")
    private Configuration configuration = new Configuration();

    public static final KafkaCredentialsCacheImpl INSTANCE = new KafkaCredentialsCacheImpl();

    public synchronized void init(Configuration configuration) {
        this.configuration = configuration.clone();
    }

    /**
     * Called when new credentials are available.
     *
     * @param credentialsByJobId The full map of credentials exposed by the cache.
     */
    @Override
    public synchronized void onNewCredentialsObtained(
            Map<JobID, KafkaCredentials> credentialsByJobId) {
        this.credentialsByJobId = new HashMap<>(credentialsByJobId);
        notifyAll();
    }

    /**
     * Gets credentials for the given job, possibly blocking until they are available or a timeout
     * occurs.
     *
     * @param jobID The job to get credentials for
     * @return The credentials if they are available
     */
    @Override
    public Optional<KafkaCredentials> getCredentials(JobID jobID) {
        return getCredentials(SystemClock.getInstance(), jobID);
    }

    @VisibleForTesting
    synchronized Optional<KafkaCredentials> getCredentials(Clock clock, JobID jobID) {
        long timeUpMs =
                clock.absoluteTimeMillis()
                        + configuration.getLong(KafkaCredentialsCacheOptions.RECEIVE_TIMEOUT_MS);
        KafkaCredentials kafkaCredentials;
        long remaining;
        while ((kafkaCredentials = credentialsByJobId.get(jobID)) == null
                && (remaining = timeUpMs - clock.absoluteTimeMillis()) > 0) {
            try {
                wait(remaining);
            } catch (Throwable t) {
                throw new FlinkRuntimeException("Error retrieving credentials", t);
            }
        }
        if (kafkaCredentials != null) {
            return Optional.of(kafkaCredentials);
        } else {
            if (configuration.getBoolean(KafkaCredentialsCacheOptions.RECEIVE_ERROR_ON_TIMEOUT)) {
                throw new FlinkRuntimeException("Timed out while waiting for credentials");
            } else {
                return Optional.empty();
            }
        }
    }

    /** Singleton, so no public constructor. */
    protected KafkaCredentialsCacheImpl() {}
}
