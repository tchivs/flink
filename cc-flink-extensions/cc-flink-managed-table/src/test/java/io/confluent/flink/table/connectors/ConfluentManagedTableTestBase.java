/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.DockerImageVersions;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** {@link AbstractTestBase} for Confluent-native tables. */
@Confluent
@Testcontainers
public class ConfluentManagedTableTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentManagedTableTestBase.class);

    @Container
    @SuppressWarnings("resource")
    private static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA)) {
                @Override
                protected void doStart() {
                    super.doStart();
                    if (LOG.isInfoEnabled()) {
                        followOutput(new Slf4jLogConsumer(LOG));
                    }
                }
            }.withEmbeddedZookeeper()
                    .withNetworkAliases("kafka")
                    .withEnv(
                            "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                            String.valueOf(Duration.ofHours(2).toMillis()))
                    .withEnv("KAFKA_LOG_RETENTION_MS", "-1");

    private static final int PARALLELISM = 4;

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tableEnv;

    @BeforeEach
    void setupEnvironments() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tableEnv = StreamTableEnvironment.create(env);
    }

    protected String getBootstrapServers() {
        return KAFKA_CONTAINER.getBootstrapServers();
    }

    protected String createTopic(String topic) {
        final String uniqueName = topic + UUID.randomUUID();
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(
                            Collections.singletonList(
                                    new NewTopic(uniqueName, PARALLELISM, (short) 1)))
                    .all()
                    .get();
            return uniqueName;
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Could not create topic %s.", topic), e);
        }
    }

    protected void deleteTopic(String topic) {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Could not delete topic %s.", topic), e);
        }
    }
}
