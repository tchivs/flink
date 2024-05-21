/*
 * Copyright 2024 Confluent Inc.
 */

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Custom KubernetesStateHandleStore specifically for checkpoints that additionally writes the
 * checkpoint path.
 */
@Confluent
public class KubernetesCheckpointStateHandleStore
        extends KubernetesStateHandleStore<CompletedCheckpoint> {

    public static final String CHECKPOINT_PATH_KEY_PREFIX = "checkpointPath-";
    private final CheckpointStoreUtil checkpointStoreUtil;

    public KubernetesCheckpointStateHandleStore(
            CheckpointStoreUtil checkpointStoreUtil,
            FlinkKubeClient kubeClient,
            String configMapName,
            RetrievableStateStorageHelper<CompletedCheckpoint> storage,
            Predicate<String> configMapKeyFilter,
            @Nullable String lockIdentity) {
        super(kubeClient, configMapName, storage, configMapKeyFilter, lockIdentity);
        this.checkpointStoreUtil = checkpointStoreUtil;
    }

    @VisibleForTesting
    static String getCheckpointPathKey(long checkpointId) {
        return CHECKPOINT_PATH_KEY_PREFIX + String.format("%019d", checkpointId);
    }

    @Override
    protected void onAdd(CompletedCheckpoint checkpoint, Map<String, String> data) {
        if (checkpoint.getExternalPointer() != null) {
            data.put(
                    getCheckpointPathKey(checkpoint.getCheckpointID()),
                    checkpoint.getExternalPointer());
        }
    }

    @Override
    protected void onDelete(String key, Map<String, String> data) {
        data.remove(getCheckpointPathKey(checkpointStoreUtil.nameToCheckpointID(key)));
    }
}
