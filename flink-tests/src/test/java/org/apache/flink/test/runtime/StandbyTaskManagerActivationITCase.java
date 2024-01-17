/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.test.runtime;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerConfluentOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerConfluentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.StandbyTaskManagerActivationHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.StandbyTaskManagerActivationRequestBody;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.testutils.DispatcherProcess;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.RetryOnException;
import org.apache.flink.testutils.junit.extensions.retry.RetryExtension;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;

/**
 * Verify behavior when TaskManagers start in standby mode and JobManager takes over them by
 * providing the correct configuration.
 */
@Confluent
@ExtendWith({TestLoggerExtension.class, RetryExtension.class})
public class StandbyTaskManagerActivationITCase {

    private static final String LOCALHOST = "localhost";
    private static ZooKeeperTestEnvironment zooKeeper;

    private static final Duration TEST_TIMEOUT = Duration.ofMinutes(5);

    private static final Random RANDOM = new Random();

    @RegisterExtension
    public static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @TempDir private File temporaryFolder;

    @BeforeAll
    public static void setup() {
        zooKeeper = new ZooKeeperTestEnvironment(1);
    }

    @BeforeEach
    public void cleanUp() throws Exception {
        zooKeeper.deleteAll();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (zooKeeper != null) {
            zooKeeper.shutdown();
        }
    }

    @TestTemplate
    @RetryOnException(times = 3, exception = BindException.class)
    public void testTaskManagerPauseOnStart() throws Exception {
        // given: The config for zookeeper.
        Configuration zooKeeperHAConfig =
                ZooKeeperTestUtils.createZooKeeperHAConfig(
                        zooKeeper.getConnectString(), temporaryFolder.getPath());

        Configuration jobManagerConfig = new Configuration(zooKeeperHAConfig);
        // and: STANDBY_TASK_MANAGER_OVERRIDE_OPTIONS with HA_MODE which should force JobManager to
        // send its HA_MODE config to TaskManager when it takes over standby TaskManager.
        jobManagerConfig.set(
                JobManagerConfluentOptions.STANDBY_TASK_MANAGER_OVERRIDE_OPTIONS,
                Collections.singletonList(HighAvailabilityOptions.HA_MODE.key()));

        try (AutoCloseableRegistry autoCloseableRegistry = new AutoCloseableRegistry()) {
            final Deadline deadline = Deadline.fromNow(TEST_TIMEOUT);

            // when: JobManager starts.
            DispatcherProcess jobManager = new DispatcherProcess(0, jobManagerConfig);
            jobManager.startProcess();
            autoCloseableRegistry.registerCloseable(jobManager::destroy);

            // and: Two TaskManagers start in standby mode and without HA_MODE which makes them
            // impossible to connect to JobManager.
            final PluginManager pluginManager =
                    PluginUtils.createPluginManagerFromRootFolder(zooKeeperHAConfig);

            final NetUtils.Port availablePort = NetUtils.getAvailablePort();
            autoCloseableRegistry.registerCloseable(availablePort);
            Configuration taskManagerConfig1 =
                    createTaskManagerConfigWithoutHaMode(zooKeeperHAConfig, 1)
                            .set(TaskManagerConfluentOptions.STANDBY_RPC_PORT, 0)
                            // configure a specific but random RPC port, which fails the tests
                            // if it were used by both the standby and main rpc service
                            .set(
                                    TaskManagerOptions.RPC_PORT,
                                    String.valueOf(availablePort.getPort()));
            TaskManagerRunner taskManagerRunner1 =
                    new TaskManagerRunner(
                            taskManagerConfig1,
                            pluginManager,
                            TaskManagerRunner::createTaskExecutorService);
            autoCloseableRegistry.registerCloseable(taskManagerRunner1::close);
            final CheckedThread taskManagerRunner1Thread = checkedThread(taskManagerRunner1::start);
            taskManagerRunner1Thread.start();

            Configuration taskManagerConfig2 =
                    createTaskManagerConfigWithoutHaMode(zooKeeperHAConfig, 3)
                            .set(TaskManagerConfluentOptions.STANDBY_RPC_PORT, 0);
            TaskManagerRunner taskManagerRunner2 =
                    new TaskManagerRunner(
                            taskManagerConfig2,
                            pluginManager,
                            TaskManagerRunner::createTaskExecutorService);
            autoCloseableRegistry.registerCloseable(taskManagerRunner2::close);
            final CheckedThread taskManagerRunner2Thread = checkedThread(taskManagerRunner2::start);
            taskManagerRunner2Thread.start();

            // then: Zero TaskManagers should connect to JobManager.
            HighAvailabilityServices highAvailabilityServices =
                    HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
                            zooKeeperHAConfig,
                            EXECUTOR_RESOURCE.getExecutor(),
                            NoOpFatalErrorHandler.INSTANCE);
            autoCloseableRegistry.registerCloseable(
                    highAvailabilityServices::closeAndCleanupAllData);

            final DispatcherGateway dispatcherGateway =
                    retrieveDispatcherGateway(
                            autoCloseableRegistry, zooKeeperHAConfig, highAvailabilityServices);

            waitForTaskManagers(0, 0, dispatcherGateway, deadline.timeLeft());

            // when: Takes over one of standby TaskManagers with providing HA_MODE option.
            int jobManagerRestPort = retrieveJobManagerRestPort(highAvailabilityServices);
            final RestClient restClient =
                    new RestClient(zooKeeperHAConfig, EXECUTOR_RESOURCE.getExecutor());

            waitUntilCondition(() -> taskManagerRunner2.getStandbyTaskManager() != null);
            activateTaskManager(
                    restClient,
                    jobManagerRestPort,
                    taskManagerRunner2.getStandbyTaskManager().getRpcService().getPort());

            // then: One TaskManager with 3 slots should join the JobManager.
            waitForTaskManagersWithAbort(
                    1,
                    3,
                    dispatcherGateway,
                    () -> taskManagerRunner2Thread.trySync(1),
                    deadline.timeLeft());

            // when: Takes over the second TaskManager with providing HA_MODE.
            waitUntilCondition(() -> taskManagerRunner1.getStandbyTaskManager() != null);
            activateTaskManager(
                    restClient,
                    jobManagerRestPort,
                    taskManagerRunner1.getStandbyTaskManager().getRpcService().getPort());

            // then: The second TaskManager with 1 slot should join the JobManager.
            waitForTaskManagersWithAbort(
                    2,
                    4,
                    dispatcherGateway,
                    () -> taskManagerRunner1Thread.trySync(1),
                    deadline.timeLeft());
        }
    }

    private static CheckedThread checkedThread(ThrowingRunnable<Exception> r) {
        return new CheckedThread() {
            @Override
            public void go() throws Exception {
                r.run();
            }
        };
    }

    private static void activateTaskManager(RestClient restClient, int jobManagerRestPort, int port)
            throws ExecutionException, InterruptedException {
        FutureUtils.retryWithDelay(
                        () -> {
                            try {
                                return restClient.sendRequest(
                                        LOCALHOST,
                                        jobManagerRestPort,
                                        StandbyTaskManagerActivationHeaders.getInstance(),
                                        EmptyMessageParameters.getInstance(),
                                        new StandbyTaskManagerActivationRequestBody(
                                                LOCALHOST, String.valueOf(port)));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        },
                        new FixedRetryStrategy(1000, Duration.ofMillis(50)),
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))
                .get();
    }

    private static Configuration createTaskManagerConfigWithoutHaMode(
            Configuration zooKeeperHAConfig, int numSlots) {
        Configuration config = new Configuration(zooKeeperHAConfig);

        // Task manager configuration
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, numSlots);
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("3200k"));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("3200k"));
        config.set(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS, 16);
        config.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse("128m"));
        config.set(TaskManagerOptions.CPU_CORES, 1.0);
        config.set(TaskManagerConfluentOptions.STANDBY_MODE, true);
        config.set(TaskManagerOptions.HOST, LOCALHOST);
        TaskExecutorResourceUtils.adjustForLocalExecution(config);

        // Removing HA_MODE to guarantee that TaskManager never find JobManager with this
        // configuration. The only way to connect to JobManager is to dynamically override with
        // HA_MODE via JobManager.
        config.removeConfig(HighAvailabilityOptions.HA_MODE);

        return config;
    }

    private static DispatcherGateway retrieveDispatcherGateway(
            AutoCloseableRegistry autoCloseableRegistry,
            Configuration zooKeeperHAConfig,
            HighAvailabilityServices highAvailabilityServices)
            throws Exception {
        TestingListener leaderListener = new TestingListener();
        LeaderRetrievalService leaderRetrievalService =
                highAvailabilityServices.getDispatcherLeaderRetriever();
        try {
            leaderRetrievalService.start(leaderListener);

            leaderListener.waitForNewLeader();
        } finally {
            leaderRetrievalService.stop();
        }

        String leaderAddress = leaderListener.getAddress();
        UUID leaderId = leaderListener.getLeaderSessionID();

        RpcSystem rpcSystem = RpcSystem.load();
        autoCloseableRegistry.registerCloseable(rpcSystem);
        final RpcService rpcService =
                rpcSystem.remoteServiceBuilder(zooKeeperHAConfig, LOCALHOST, "0").createAndStart();
        autoCloseableRegistry.registerCloseable(rpcService);

        final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture =
                rpcService.connect(
                        leaderAddress, DispatcherId.fromUuid(leaderId), DispatcherGateway.class);

        return dispatcherGatewayFuture.get();
    }

    private static int retrieveJobManagerRestPort(HighAvailabilityServices highAvailabilityServices)
            throws Exception {
        TestingListener webLeaderListener = new TestingListener();
        LeaderRetrievalService leaderRetriever =
                highAvailabilityServices.getClusterRestEndpointLeaderRetriever();
        try {
            leaderRetriever.start(webLeaderListener);
            webLeaderListener.waitForNewLeader();
        } finally {
            leaderRetriever.stop();
        }
        String restUri = webLeaderListener.getAddress();
        String[] split = restUri.split(":");
        return Integer.parseInt(split[split.length - 1]);
    }

    private static void waitForTaskManagers(
            int numberOfTaskManagers,
            int numberOfSlots,
            DispatcherGateway dispatcherGateway,
            Duration timeLeft)
            throws Exception {
        waitForTaskManagersWithAbort(
                numberOfTaskManagers, numberOfSlots, dispatcherGateway, () -> {}, timeLeft);
    }

    private static void waitForTaskManagersWithAbort(
            int numberOfTaskManagers,
            int numberOfSlots,
            DispatcherGateway dispatcherGateway,
            ThrowingRunnable<Exception> abortCondition,
            Duration timeLeft)
            throws Exception {

        final Deadline deadline = Deadline.fromNow(Duration.ofMillis(timeLeft.toMillis()));
        for (; deadline.hasTimeLeft(); abortCondition.run()) {
            ClusterOverview clusterOverview =
                    dispatcherGateway
                            .requestClusterOverview(Time.milliseconds(timeLeft.toMillis()))
                            .join();

            if (clusterOverview.getNumTaskManagersConnected() == numberOfTaskManagers
                    && clusterOverview.getNumSlotsTotal() == numberOfSlots) {
                return;
            }
            Thread.sleep(50L);
        }
    }
}
