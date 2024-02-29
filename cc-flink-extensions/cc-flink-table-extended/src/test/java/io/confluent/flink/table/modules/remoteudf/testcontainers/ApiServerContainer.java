/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.testcontainers;

import io.confluent.flink.apiserver.client.ApiClient;
import io.confluent.flink.apiserver.client.ComputeV1alphaApi;
import io.confluent.flink.apiserver.client.CoreV1Api;
import io.confluent.flink.apiserver.client.SqlV1Api;
import io.confluent.flink.apiserver.client.SqlV2alphaApi;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** A test container for the ApiServer using project's apiserver version. */
public class ApiServerContainer extends GenericContainer<ApiServerContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse(
                    "519856050701.dkr.ecr.us-west-2.amazonaws.com/docker/prod/confluentinc/"
                            + "cc-flink-cp-apiserver");

    private static final String APISERVER_VERSION_PROPERTY = "apiserver.image.version";
    private static final String APISERVER_VERSION = System.getProperty(APISERVER_VERSION_PROPERTY);

    private CoreV1Api coreV1Api;
    private SqlV1Api sqlV1Api;
    private SqlV2alphaApi sqlV2alphaApi;
    private ComputeV1alphaApi computeV1alphaApi;
    private static final int PORT = 8080;

    public ApiServerContainer() {
        super(DEFAULT_IMAGE_NAME.withTag("v" + APISERVER_VERSION));
    }

    public void start() {
        System.out.println("Starting ApiServer container v" + APISERVER_VERSION);
        withNetworkAliases("apiserver-test");
        withClasspathResourceMapping(
                "apiserver-config.yaml", "/etc/config.yaml", BindMode.READ_WRITE);
        withCommand("--config /etc/config.yaml run");
        super.withExposedPorts(PORT);

        super.start();

        final ApiClient apiClient =
                new ApiClient().setBasePath(getHostAddress()).setDebugging(true);

        coreV1Api = new CoreV1Api(apiClient);
        sqlV1Api = new SqlV1Api(apiClient);
        sqlV2alphaApi = new SqlV2alphaApi(apiClient);
        computeV1alphaApi = new ComputeV1alphaApi(apiClient);

        System.out.println("ApiServer started at " + getHostAddress());
    }

    public void stop() {
        super.stop();
    }

    public String getHostAddress() {
        return "http://" + super.getHost() + ":" + super.getMappedPort(PORT);
    }

    public ApiClient getClient() {
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(getHostAddress());
        return apiClient;
    }

    public CoreV1Api getCoreV1Api() {
        return coreV1Api;
    }

    public SqlV1Api getSqlV1Api() {
        return sqlV1Api;
    }

    public SqlV2alphaApi getSqlV2alphaApi() {
        return sqlV2alphaApi;
    }

    public ComputeV1alphaApi getComputeV1alphaApi() {
        return computeV1alphaApi;
    }
}
