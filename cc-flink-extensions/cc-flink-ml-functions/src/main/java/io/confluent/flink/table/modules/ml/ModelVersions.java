/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Map;

/** This class encapsulates versions of a model. */
public class ModelVersions implements Serializable {
    private final String modelName;
    // Map of (Version_id, (IsDefaultVersion, ModelProperties)).
    private final Map<String, Tuple2<Boolean, Map<String, String>>> versions;

    public ModelVersions(
            String modelName, Map<String, Tuple2<Boolean, Map<String, String>>> versions) {
        this.modelName = modelName;
        this.versions = versions;
    }

    public String getModelName() {
        return modelName;
    }

    public Map<String, Tuple2<Boolean, Map<String, String>>> getVersions() {
        return versions;
    }

    public Map<String, String> getVersionProperties(String versionId) {
        // Assume there is only one version in the model.
        if (versionId == null || versionId.isEmpty()) {
            if (versions != null && versions.size() == 1) {
                return versions.values().iterator().next().f1;
            }
            return null;
        }
        if (versionId.equalsIgnoreCase("DEFAULT")) {
            for (Map.Entry<String, Tuple2<Boolean, Map<String, String>>> entry :
                    versions.entrySet()) {
                if (entry.getValue().f0) {
                    return entry.getValue().f1;
                }
            }
            return null;
        }
        return versions.get(versionId).f1;
    }
}
