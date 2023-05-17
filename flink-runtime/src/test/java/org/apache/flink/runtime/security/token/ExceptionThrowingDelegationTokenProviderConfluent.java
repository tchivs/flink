/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.security.token;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProviderConfluent;

import java.util.HashMap;

/** A ExceptionThrowingDelegationTokenProvider with some additional methods. */
@Confluent
public class ExceptionThrowingDelegationTokenProviderConfluent
        extends ExceptionThrowingDelegationTokenProvider
        implements DelegationTokenProviderConfluent {

    public static final ThreadLocal<HashMap<JobID, Configuration>> REGISTERED_JOBS =
            ThreadLocal.withInitial(HashMap::new);
    public static final ThreadLocal<Boolean> SHOULD_REOBTAIN_TOKEN =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static final ThreadLocal<Boolean> STOPPED = ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static final ThreadLocal<Boolean> SHOULD_THROW_REGISTER =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static final ThreadLocal<Boolean> SHOULD_THROW_UNREGISTER =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    public static void reset() {
        ExceptionThrowingDelegationTokenProvider.reset();
        REGISTERED_JOBS.set(new HashMap<>());
        SHOULD_REOBTAIN_TOKEN.set(false);
        STOPPED.set(false);
        SHOULD_THROW_REGISTER.set(false);
        SHOULD_THROW_UNREGISTER.set(false);
    }

    @Override
    public String serviceName() {
        return "throw-Confluent";
    }

    @Override
    public boolean registerJob(JobID jobId, Configuration jobConfiguration) {
        if (SHOULD_THROW_REGISTER.get()) {
            throw new IllegalArgumentException();
        }
        REGISTERED_JOBS.get().put(jobId, jobConfiguration);
        return SHOULD_REOBTAIN_TOKEN.get();
    }

    @Override
    public void unregisterJob(JobID jobId) {
        if (SHOULD_THROW_UNREGISTER.get()) {
            throw new IllegalArgumentException();
        }
        REGISTERED_JOBS.get().remove(jobId);
    }

    @Override
    public void stop() {
        STOPPED.set(true);
    }
}
