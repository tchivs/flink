/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.security.token;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorServiceConfluent;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests DefaultDelegationTokenManager's additional Confluent functionality. */
@Confluent
public class DefaultDelegationTokenManagerConfluentTest {

    @RegisterExtension
    static final ContextClassLoaderExtension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            DelegationTokenProvider.class,
                            ExceptionThrowingDelegationTokenProviderConfluent.class.getName())
                    .withServiceEntry(
                            DelegationTokenReceiver.class,
                            ExceptionThrowingDelegationTokenReceiverConfluent.class.getName())
                    .build();

    @BeforeEach
    public void beforeEach() {
        ExceptionThrowingDelegationTokenProviderConfluent.reset();
        ExceptionThrowingDelegationTokenReceiverConfluent.reset();
    }

    @AfterEach
    public void afterEach() {
        ExceptionThrowingDelegationTokenProviderConfluent.reset();
        ExceptionThrowingDelegationTokenReceiverConfluent.reset();
    }

    @Test
    public void registerJob() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorServiceConfluent scheduler =
                new ManuallyTriggeredScheduledExecutorServiceConfluent();

        ExceptionThrowingDelegationTokenProviderConfluent.addToken.set(true);
        Configuration configuration = new Configuration();
        AtomicInteger startTokensUpdateCallCount = new AtomicInteger(0);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler) {
                    @Override
                    void startTokensUpdate() {
                        startTokensUpdateCallCount.incrementAndGet();
                        super.startTokensUpdate();
                    }
                };
        ExceptionThrowingDelegationTokenProviderConfluent.SHOULD_REOBTAIN_TOKEN.set(true);

        JobID jobID = JobID.generate();
        Configuration jobConfiguration = new Configuration();
        delegationTokenManager.registerJob(jobID, jobConfiguration);
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        assertEquals(1, startTokensUpdateCallCount.get());
        assertEquals(
                1, ExceptionThrowingDelegationTokenProviderConfluent.REGISTERED_JOBS.get().size());
        delegationTokenManager.unregisterJob(jobID);
        assertEquals(
                0, ExceptionThrowingDelegationTokenProviderConfluent.REGISTERED_JOBS.get().size());
    }

    @Test
    public void testStop() {
        Configuration configuration = new Configuration();
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);
        delegationTokenManager.stop();
        assertTrue(ExceptionThrowingDelegationTokenProviderConfluent.STOPPED.get());
    }

    @Test
    public void exceptionRegisterJob() throws Exception {
        Configuration configuration = new Configuration();
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        JobID jobID = JobID.generate();
        Configuration jobConfiguration = new Configuration();
        delegationTokenManager.registerJob(jobID, jobConfiguration);
        assertEquals(
                1, ExceptionThrowingDelegationTokenProviderConfluent.REGISTERED_JOBS.get().size());

        ExceptionThrowingDelegationTokenProviderConfluent.SHOULD_THROW_REGISTER.set(true);
        assertThrows(
                IllegalArgumentException.class,
                () -> delegationTokenManager.registerJob(jobID, jobConfiguration));

        assertEquals(
                0, ExceptionThrowingDelegationTokenProviderConfluent.REGISTERED_JOBS.get().size());
    }

    @Test
    public void exceptionUnregisterJob() throws Exception {
        Configuration configuration = new Configuration();
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        JobID jobID = JobID.generate();
        Configuration jobConfiguration = new Configuration();
        delegationTokenManager.registerJob(jobID, jobConfiguration);
        assertEquals(
                1, ExceptionThrowingDelegationTokenProviderConfluent.REGISTERED_JOBS.get().size());

        ExceptionThrowingDelegationTokenProviderConfluent.SHOULD_THROW_UNREGISTER.set(true);
        delegationTokenManager.unregisterJob(jobID);
    }
}
