/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerConfluentOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenReceiverRepository;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/** Tests for the {@link TaskManagerRunner} with STANDBY_MODE option. */
@Confluent
@Timeout(30)
public class StandbyTaskManagerRunnerTest extends TestLogger {

    private TaskManagerRunner taskManagerRunner;

    @AfterEach
    public void after() throws Exception {
        if (taskManagerRunner != null) {
            taskManagerRunner.close();
        }
    }

    @ParameterizedTest
    @MethodSource("standbySetupFailsIfNotAllOptionsConfiguredArguments")
    public void testStandbySetupFailsIfNotAllOptionsConfigured(ConfigOption<?> configuredOption)
            throws Exception {
        // given: Configured TaskManager in standby mode.
        final Configuration configuration =
                createConfiguration().set(TaskManagerConfluentOptions.STANDBY_MODE, true);
        if (configuredOption.equals(TaskManagerConfluentOptions.STANDBY_HOST)) {
            configuration.set(TaskManagerConfluentOptions.STANDBY_HOST, "localhost");
        } else if (configuredOption.equals(TaskManagerConfluentOptions.STANDBY_RPC_PORT)) {
            configuration.set(TaskManagerConfluentOptions.STANDBY_RPC_PORT, 0);
        }

        // when: Start the TaskManagerRunner.
        DataCollectorExecutorServiceFactory taskExecutorServiceFactory =
                new DataCollectorExecutorServiceFactory();
        TaskManagerRunner taskManagerRunner1 =
                createTaskManagerRunner(configuration, taskExecutorServiceFactory);

        // then: the start-up should fail because no standby port was configured
        assertThatThrownBy(taskManagerRunner1::start)
                .isInstanceOf(IllegalConfigurationException.class);
    }

    private static Stream<ConfigOption<?>> standbySetupFailsIfNotAllOptionsConfiguredArguments() {
        return Stream.of(
                TaskManagerConfluentOptions.STANDBY_HOST,
                TaskManagerConfluentOptions.STANDBY_RPC_PORT);
    }

    @Test
    public void testTakingOverStandbyTaskManagers() throws Exception {
        // given: Configured TaskManager in standby mode.
        final String standbyHost = "standbyhost";
        final Configuration configuration =
                createConfiguration()
                        .set(TaskManagerConfluentOptions.STANDBY_MODE, true)
                        .set(TaskManagerConfluentOptions.STANDBY_HOST, standbyHost)
                        .set(TaskManagerConfluentOptions.STANDBY_RPC_PORT, 0);

        // and: The one config that should be overridden and one should stay untouched.
        ConfigOption<String> keyForOverride =
                ConfigOptions.key("KEY_FOR_OVERRIDE").stringType().noDefaultValue();
        configuration.set(keyForOverride, "OVERRIDE_ORIGINAL_VALUE");
        ConfigOption<String> untouchedKey =
                ConfigOptions.key("UNTOUCHED_KEY").stringType().noDefaultValue();
        configuration.set(untouchedKey, "UNTOUCHED_ORIGINAL_VALUE");

        // when: Start the TaskManagerRunner.
        DataCollectorExecutorServiceFactory taskExecutorServiceFactory =
                new DataCollectorExecutorServiceFactory();
        taskManagerRunner =
                createAndStartTaskManagerRunner(configuration, taskExecutorServiceFactory);
        waitUntilCondition(() -> taskManagerRunner.getStandbyTaskManager() != null);

        // then: Runner should be switched to standby mode and taskExecutorServiceFactory should not
        // be called.
        assertEquals(0, taskExecutorServiceFactory.callCounter.get());

        // when: Activate the TaskManagerRunner with a new config.
        Configuration overrideConfig = new Configuration();
        overrideConfig.setString(keyForOverride, "OVERRIDE_NEW_VALUE");
        StandbyTaskManagerGateway configuratorGateway =
                taskManagerRunner
                        .getStandbyTaskManager()
                        .getSelfGateway(StandbyTaskManagerGateway.class);
        configuratorGateway.activate(overrideConfig);

        waitUntilCondition(() -> taskExecutorServiceFactory.callCounter.get() > 0);

        // then: taskExecutorServiceFactory should be called with new configuration.
        assertEquals(1, taskExecutorServiceFactory.callCounter.get());
        assertEquals(
                "OVERRIDE_NEW_VALUE",
                taskExecutorServiceFactory.lastReceivedConfig.get(keyForOverride));
        assertEquals(
                "UNTOUCHED_ORIGINAL_VALUE",
                taskExecutorServiceFactory.lastReceivedConfig.get(untouchedKey));

        // when: Activate the TaskManagerRunner for the second time.
        overrideConfig = new Configuration();
        overrideConfig.setString(keyForOverride, "OVERRIDE_NEW_VALUE_AGAIN");

        configuratorGateway.activate(overrideConfig);

        waitUntilCondition(() -> taskExecutorServiceFactory.callCounter.get() > 0);

        // then: taskExecutorServiceFactory should NOT be called for the second time.
        assertEquals(1, taskExecutorServiceFactory.callCounter.get());
        assertEquals(
                "OVERRIDE_NEW_VALUE",
                taskExecutorServiceFactory.lastReceivedConfig.get(keyForOverride));
        assertEquals(
                "UNTOUCHED_ORIGINAL_VALUE",
                taskExecutorServiceFactory.lastReceivedConfig.get(untouchedKey));
        assertNotEquals(standbyHost, taskExecutorServiceFactory.lastRpcService.getAddress());
    }

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, "localhost");
        return TaskExecutorResourceUtils.adjustForLocalExecution(configuration);
    }

    private static TaskManagerRunner createTaskManagerRunner(
            final Configuration configuration,
            TaskManagerRunner.TaskExecutorServiceFactory taskExecutorServiceFactory)
            throws Exception {
        final PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configuration);
        return new TaskManagerRunner(configuration, pluginManager, taskExecutorServiceFactory);
    }

    private static TaskManagerRunner createAndStartTaskManagerRunner(
            final Configuration configuration,
            TaskManagerRunner.TaskExecutorServiceFactory taskExecutorServiceFactory)
            throws Exception {
        TaskManagerRunner taskManagerRunner =
                createTaskManagerRunner(configuration, taskExecutorServiceFactory);
        Thread startThread = new Thread(ThrowingRunnable.unchecked(taskManagerRunner::start));
        startThread.start();
        return taskManagerRunner;
    }

    private static class DataCollectorExecutorServiceFactory
            implements TaskManagerRunner.TaskExecutorServiceFactory {

        private final AtomicInteger callCounter = new AtomicInteger();
        private volatile Configuration lastReceivedConfig;
        private volatile RpcService lastRpcService;

        @Override
        public TaskManagerRunner.TaskExecutorService createTaskExecutor(
                Configuration configuration,
                ResourceID resourceID,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                MetricRegistry metricRegistry,
                BlobCacheService blobCacheService,
                boolean localCommunicationOnly,
                ExternalResourceInfoProvider externalResourceInfoProvider,
                WorkingDirectory workingDirectory,
                FatalErrorHandler fatalErrorHandler,
                DelegationTokenReceiverRepository delegationTokenReceiverRepository) {
            callCounter.incrementAndGet();
            lastReceivedConfig = configuration;
            lastRpcService = rpcService;
            return TestingTaskExecutorService.newBuilder().setStartRunnable(() -> {}).build();
        }
    }
}
