/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Request with the connection data of the standby TaskManager. */
@Confluent
public class StandbyTaskManagerActivationRequestBody implements RequestBody {

    private static final String FIELD_NAME_RPC_HOST = "rpcHost";
    private static final String FIELD_NAME_RPC_PORT = "rpcPort";

    @JsonProperty(FIELD_NAME_RPC_HOST)
    private final String rpcHost;

    @JsonProperty(FIELD_NAME_RPC_PORT)
    private final String rpcPort;

    @JsonCreator
    public StandbyTaskManagerActivationRequestBody(
            @JsonProperty(FIELD_NAME_RPC_HOST) String rpcHost,
            @JsonProperty(FIELD_NAME_RPC_PORT) String rpcPort) {
        this.rpcHost = rpcHost;
        this.rpcPort = rpcPort;
    }

    @JsonIgnore
    public String getRpcHost() {
        return rpcHost;
    }

    @JsonIgnore
    public String getRpcPort() {
        return rpcPort;
    }
}
