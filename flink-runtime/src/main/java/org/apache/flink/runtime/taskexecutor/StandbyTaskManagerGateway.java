/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcGateway;

/** The gateway for the standby TaskManager that allows to configure it before activation. */
@Confluent
public interface StandbyTaskManagerGateway extends RpcGateway {

    /**
     * Transit this TaskManager from standby to active mode with given override configuration.
     *
     * @param overrides The configuration that should override the existing TaskManager
     *     configuration.
     */
    void activate(Configuration overrides);
}
