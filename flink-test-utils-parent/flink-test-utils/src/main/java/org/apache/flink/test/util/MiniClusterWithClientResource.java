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

package org.apache.flink.test.util;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.ExceptionUtils;

/** Starts a Flink mini cluster as a resource and registers the StreamExecutionEnvironment. */
public class MiniClusterWithClientResource extends MiniClusterResource {

    private ClusterClient<?> clusterClient;
    private RestClusterClient<MiniClusterClient.MiniClusterId> restClusterClient;

    private TestStreamEnvironment streamExecutionEnvironment;

    public MiniClusterWithClientResource(
            final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
        super(miniClusterResourceConfiguration);
    }

    public ClusterClient<?> getClusterClient() {
        return clusterClient;
    }

    /**
     * Returns a {@link RestClusterClient} that can be used to communicate with this mini cluster.
     * Only use this if the client returned via {@link #getClusterClient()} does not fulfill your
     * needs.
     */
    public RestClusterClient<?> getRestClusterClient() throws Exception {
        return restClusterClient;
    }

    public TestStreamEnvironment getTestStreamEnvironment() {
        return streamExecutionEnvironment;
    }

    @Override
    public void before() throws Exception {
        super.before();

        clusterClient = createMiniClusterClient();
        restClusterClient = createRestClusterClient();

        TestStreamEnvironment.setAsContext(getMiniCluster(), getNumberSlots());
        streamExecutionEnvironment = new TestStreamEnvironment(getMiniCluster(), getNumberSlots());
    }

    @Override
    public void after() {
        log.info("Finalization triggered: Cluster shutdown is going to be initiated.");
        TestStreamEnvironment.unsetAsContext();

        Exception exception = null;

        if (clusterClient != null) {
            try {
                clusterClient.close();
            } catch (Exception e) {
                exception = e;
            }
        }

        clusterClient = null;

        if (restClusterClient != null) {
            try {
                restClusterClient.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        restClusterClient = null;

        super.after();

        if (exception != null) {
            log.warn("Could not properly shut down the MiniClusterWithClientResource.", exception);
        }
    }

    private MiniClusterClient createMiniClusterClient() {
        return new MiniClusterClient(getClientConfiguration(), getMiniCluster());
    }

    private RestClusterClient<MiniClusterClient.MiniClusterId> createRestClusterClient()
            throws Exception {
        return new RestClusterClient<>(
                getClientConfiguration(), MiniClusterClient.MiniClusterId.INSTANCE);
    }
}
