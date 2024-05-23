/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Utility for deserializing the open payload for invocations. */
public class OpenPayload {

    private static final Logger LOG = Logger.getLogger(OpenPayload.class.getName());

    private final RemoteUdfSpec remoteUdfSpec;
    private final Configuration configuration;

    public OpenPayload(RemoteUdfSpec remoteUdfSpec, Configuration configuration) {
        this.remoteUdfSpec = remoteUdfSpec;
        this.configuration = configuration;
    }

    public static OpenPayload open(byte[] openPayload, ClassLoader classLoader) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(openPayload);
        RemoteUdfSpec remoteUdfSpec = RemoteUdfSpec.deserialize(in, classLoader);
        Configuration configuration = new Configuration();
        try {
            configuration = FlinkConfiguration.deserialize(in);
        } catch (Throwable throwable) {
            LOG.log(Level.WARNING, "Couldn't find configuration", throwable);
        }
        return new OpenPayload(remoteUdfSpec, configuration);
    }

    public RemoteUdfSpec getRemoteUdfSpec() {
        return remoteUdfSpec;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
