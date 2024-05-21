/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/** Standby TaskManager API that allows to activate TaskManager with given configuration. */
@Confluent
public class StandbyTaskManager extends RpcEndpoint implements StandbyTaskManagerGateway {
    private static final Logger LOG = LoggerFactory.getLogger(StandbyTaskManager.class);

    private static final String TASK_MANAGER_CONFIGURATOR_NAME = "standby_taskmanager";

    private final CompletableFuture<Configuration> activationFuture = new CompletableFuture<>();

    public StandbyTaskManager(RpcService rpcService) {
        super(rpcService, TASK_MANAGER_CONFIGURATOR_NAME);
        getTerminationFuture()
                .thenRun(
                        () ->
                                activationFuture.completeExceptionally(
                                        new FlinkRuntimeException(
                                                "StandbyTaskManager was shutdown before activation")));
    }

    @Override
    public void activate(Configuration overrides) {
        if (activationFuture.isDone()) {
            LOG.info("The TaskManager is already activated.");
            return;
        }

        LOG.info(
                "Received configuration for the activation: {}",
                ConfigurationUtils.hideSensitiveValues(overrides.toMap()));
        activationFuture.complete(overrides);
    }

    public CompletableFuture<Configuration> getActivationFuture() {
        return activationFuture;
    }
}
