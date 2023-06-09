/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfluentMetricOptions;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.TriConsumer;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

@Confluent
class CustomJobVariablesITCase {

    @RegisterExtension
    @Order(1)
    static final ContextClassLoaderExtension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            MetricReporterFactory.class, TestReporterFactory.class.getName())
                    .build();

    @RegisterExtension
    @Order(2)
    static final InternalMiniClusterExtension MINI_CLUSTER =
            new InternalMiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .build());

    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        MetricOptions.forReporter(configuration, "test_reporter")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporterFactory.class.getName());
        return configuration;
    }

    @Test
    void testCustomJobVariable(@InjectMiniCluster MiniCluster miniCluster) {
        final String customKey = "custom_key";
        final String customValue = "custom_value";

        final CompletableFuture<Void> assertionFuture = new CompletableFuture<>();

        TestReporter.INSTANCE.addAssertion(
                (metric, metricName, group) -> {
                    if (group.getAllVariables().containsKey(ScopeFormat.SCOPE_JOB_ID)) {
                        FutureUtils.forward(
                                CompletableFuture.runAsync(
                                        () ->
                                                assertThat(group.getAllVariables())
                                                        .containsEntry(customKey, customValue)),
                                assertionFuture);
                    }
                });

        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();

        jobGraph.getJobConfiguration()
                .set(
                        ConfluentMetricOptions.CUSTOM_METRIC_VARIABLES,
                        Collections.singletonMap(customKey, customValue));

        miniCluster.submitJob(jobGraph).join();

        assertThatNoException().isThrownBy(assertionFuture::join);
    }

    private static final class TestReporter implements MetricReporter {

        public static final TestReporter INSTANCE = new TestReporter();

        private final Collection<TriConsumer<Metric, String, MetricGroup>> assertions =
                new ArrayList<>();

        public void addAssertion(TriConsumer<Metric, String, MetricGroup> assertion) {
            assertions.add(assertion);
        }

        @Override
        public void open(MetricConfig config) {}

        @Override
        public void close() {}

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            assertions.forEach(s -> s.accept(metric, metricName, group));
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {}
    }

    /** Factory for {@link TestReporter}. */
    public static class TestReporterFactory implements MetricReporterFactory {

        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            return TestReporter.INSTANCE;
        }
    }
}
