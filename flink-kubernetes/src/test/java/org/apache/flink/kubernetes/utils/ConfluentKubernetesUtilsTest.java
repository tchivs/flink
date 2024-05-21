/*
 * Copyright 2024 Confluent Inc.
 */

package org.apache.flink.kubernetes.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KubernetesUtils}. */
@Confluent
class ConfluentKubernetesUtilsTest extends KubernetesTestBase {

    @Test
    void testWriteConfluentClusterConfigMapLabels() {
        assertThat(KubernetesUtils.getConfigMapLabels(CLUSTER_ID, "type"))
                .containsEntry(ConfluentConstants.CONFLUENT_CLUSTER_ID_LABEL_KEY, CLUSTER_ID);
    }

    @Test
    void testWriteConfluentJobConfigMapLabels() throws FlinkException {
        final String configMap = "config-map";
        final JobID jobId = JobID.generate();

        KubernetesUtils.createConfigMapIfItDoesNotExist(
                flinkKubeClient, configMap, CLUSTER_ID, jobId);

        assertThat(flinkKubeClient.getConfigMap(configMap).get().getLabels())
                .containsEntry(ConfluentConstants.CONFLUENT_CLUSTER_ID_LABEL_KEY, CLUSTER_ID)
                .containsEntry(ConfluentConstants.CONFLUENT_JOB_ID_LABEL_KEY, jobId.toHexString());
    }
}
