/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.api;

import org.apache.flink.configuration.Configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Serializes the configuration to be used by the adapter. */
public class FlinkConfiguration {

    /**
     * Writes the given map to the output.
     *
     * @param configuration The configuration to serialize
     * @param out The output object
     * @throws IOException Thrown if an error occurs.
     */
    public static void serialize(Configuration configuration, DataOutput out) throws IOException {
        Map<String, String> map = configuration.toMap();
        out.writeInt(map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    /**
     * Reads a map from the input.
     *
     * @param in The input object to read from
     * @return The read map
     * @throws IOException Thrown if an error occurs.
     */
    public static Configuration deserialize(DataInput in) throws IOException {
        Map<String, String> map = new HashMap<>();
        int numEntries = in.readInt();
        for (int i = 0; i < numEntries; ++i) {
            map.put(in.readUTF(), in.readUTF());
        }
        return Configuration.fromMap(map);
    }
}
