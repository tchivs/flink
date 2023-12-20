/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.confluent.flink.runtime.failure.TypeFailureEnricherTableITCase.assertFailureEnricherLabelIsExpectedLabel;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test that the TypeFailureEnricher correctly labels User Kafka exceptions. */
public class TypeFailureEnricherKafkaITCase extends TestLogger {

    private static final String TOPIC1 = "topic1";
    private static final TopicPartition NON_EXISTING_TOPIC = new TopicPartition("removed", 0);

    @Test
    // Reading/subscribing to a non-existing topic should return a User exception as User probably
    // deleted the cluster/topic while the job was running
    public void testKafkaDeletedTopicException() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.createTestTopic(TOPIC1);
        AdminClient adminClient = KafkaSourceTestEnv.getAdminClient();
        final KafkaSubscriber subscriber =
                KafkaSubscriber.getTopicListSubscriber(
                        Collections.singletonList(NON_EXISTING_TOPIC.topic()));

        assertThatThrownBy(() -> subscriber.getSubscribedTopicPartitions(adminClient))
                .isInstanceOf(RuntimeException.class)
                .satisfies(anyCauseMatches(UnknownTopicOrPartitionException.class))
                .satisfies(e -> assertFailureEnricherLabelIsExpectedLabel((Exception) e, "USER"));
    }

    @Test
    // Writing to a non-existing topic should return a User exception as User probably deleted the
    // cluster/topic while the job was running
    public void testKafkaWriterCompletionException() throws Exception {
        final DummyFlinkKafkaProducer<String> producer = getStringDummyFlinkKafkaProducer();

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(producer));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>("message-1"));

        Exception timeoutException =
                new TimeoutException(
                        "Topic partition_records_by_rack_time not present in metadata after 60000 ms");

        // Message request returns an async exception
        producer.getPendingCallbacks().get(0).onCompletion(null, timeoutException);

        // Next invocation should rethrow the async exception
        assertThatThrownBy(() -> testHarness.processElement(new StreamRecord<>("mesage-2")))
                .hasMessageContaining(timeoutException.getMessage())
                .satisfies(e -> assertFailureEnricherLabelIsExpectedLabel((Exception) e, "USER"));
    }

    /** Returns a mocked String the KafkaProducer. */
    private static DummyFlinkKafkaProducer<String> getStringDummyFlinkKafkaProducer() {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:12345");
        prop.setProperty("key.serializer", ByteArraySerializer.class.getName());
        prop.setProperty("value.serializer", ByteArraySerializer.class.getName());

        return new DummyFlinkKafkaProducer<>(
                prop, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), null);
    }

    /**
     * A dummy {@link FlinkKafkaProducerBase} that allows us to mock the KafkaProducer and intercept
     * the callbacks. Inspired by: {@link
     * org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBaseTest}
     */
    private static class DummyFlinkKafkaProducer<T> extends FlinkKafkaProducerBase<T> {
        private static final long serialVersionUID = 1L;

        private static final String DUMMY_TOPIC = "dummy-topic";

        private final transient KafkaProducer<?, ?> mockProducer;
        private final transient List<Callback> pendingCallbacks = new ArrayList<>();
        private final transient MultiShotLatch flushLatch;
        private boolean isFlushed;

        @SuppressWarnings("unchecked")
        DummyFlinkKafkaProducer(
                Properties producerConfig,
                KeyedSerializationSchema<T> schema,
                FlinkKafkaPartitioner partitioner) {

            super(DUMMY_TOPIC, schema, producerConfig, partitioner);

            this.mockProducer = mock(KafkaProducer.class);
            when(mockProducer.send(any(ProducerRecord.class), any(Callback.class)))
                    .thenAnswer(
                            invocation -> {
                                pendingCallbacks.add(invocation.getArgument(1));
                                return null;
                            });

            this.flushLatch = new MultiShotLatch();
        }

        List<Callback> getPendingCallbacks() {
            return pendingCallbacks;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            isFlushed = false;

            super.snapshotState(ctx);

            // if the snapshot implementation doesn't wait until all pending records are flushed, we
            // should fail the test
            if (flushOnCheckpoint && !isFlushed) {
                throw new RuntimeException(
                        "Flushing is enabled; snapshots should be blocked until all pending records are flushed");
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
            return (KafkaProducer<K, V>) mockProducer;
        }

        @Override
        protected void flush() {
            flushLatch.trigger();

            // simply wait until the producer's pending records become zero.
            // This relies on the fact that the producer's Callback implementation
            // and pending records tracking logic is implemented correctly, otherwise
            // we will loop forever.
            while (numPendingRecords() > 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Unable to flush producer, task was interrupted");
                }
            }

            isFlushed = true;
        }
    }
}
