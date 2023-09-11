/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.CLUSTER_ID;

/**
 * Wraps {@link KubernetesHaServicesFactory} and exports {@link
 * org.apache.flink.kubernetes.configuration.KubernetesConfigOptions#CLUSTER_ID
 * kubernetes.cluster-id} as system property for logging (under {@link #PROPERTY_NAME}).
 *
 * <p>The "cluster" here means Flink Session Cluster. It is not directly related to Compute Pools,
 * nor has it to be single-tenant (although currently this is the case).
 *
 * <p>The ID is currently <a
 * href="https://github.com/confluentinc/flink-control-plane/blob/c4234426af98daadfd94440ab98e0d1a52527e62/cell-operator/controllers/k8s/controllers/component/flinkconfig/flink_conf_component.go#L325">defined
 * in FCP as JM UID</a> and is not supposed to change (e.g. on JM failover).
 */
public class ConfluentKubernetesHaServicesFactory implements HighAvailabilityServicesFactory {
    private static final Logger LOG =
            LoggerFactory.getLogger(ConfluentKubernetesHaServicesFactory.class);
    private static final String PROPERTY_NAME = "FLINK_CLUSTER_ID";

    @Override
    public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor)
            throws Exception {
        String clusterId = configuration.getString(CLUSTER_ID);
        LOG.debug("Set system property {} to {}", PROPERTY_NAME, clusterId);
        System.setProperty(PROPERTY_NAME, clusterId);
        return new KubernetesHaServicesFactory().createHAServices(configuration, executor);
    }
}
