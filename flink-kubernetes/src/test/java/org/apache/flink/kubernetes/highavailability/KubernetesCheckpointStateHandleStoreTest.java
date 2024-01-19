/*
 * Copyright 2024 Confluent Inc.
 */

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KubernetesCheckpointStateHandleStore} operations. */
@Confluent
class KubernetesCheckpointStateHandleStoreTest extends KubernetesHighAvailabilityTestBase {

    @Test
    void testCheckpointPathWrittenAndRemoved() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final JobID jobId = JobID.generate();
                            final int checkpointId = 1;
                            final TestCompletedCheckpointStorageLocation storageLocation =
                                    new TestCompletedCheckpointStorageLocation();
                            final CheckpointStoreUtil checkpointStoreUtil =
                                    KubernetesCheckpointStoreUtil.INSTANCE;

                            final String key = checkpointStoreUtil.checkpointIDToName(checkpointId);

                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointStateHandleStore store =
                                    new KubernetesCheckpointStateHandleStore(
                                            checkpointStoreUtil,
                                            flinkKubeClient,
                                            LEADER_CONFIGMAP_NAME,
                                            new TestingRetrievableStateStorageHelper<>(),
                                            i -> true,
                                            LOCK_IDENTITY);

                            store.addAndLock(
                                    key,
                                    createCompletedCheckpoint(
                                            jobId, checkpointId, storageLocation));

                            assertThat(getLeaderConfigMap().getData())
                                    .containsEntry(
                                            KubernetesCheckpointStateHandleStore
                                                    .getCheckpointPathKey(checkpointId),
                                            storageLocation.getExternalPointer());

                            store.releaseAndTryRemove(key);

                            assertThat(getLeaderConfigMap().getData())
                                    .doesNotContainKey(
                                            KubernetesCheckpointStateHandleStore
                                                    .getCheckpointPathKey(checkpointId));
                        });
            }
        };
    }

    private static CompletedCheckpoint createCompletedCheckpoint(
            JobID jobId, long checkpointId, CompletedCheckpointStorageLocation storageLocation) {
        return new CompletedCheckpoint(
                jobId,
                checkpointId,
                0,
                System.currentTimeMillis(),
                Collections.emptyMap(),
                null,
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION),
                storageLocation,
                null);
    }
}
