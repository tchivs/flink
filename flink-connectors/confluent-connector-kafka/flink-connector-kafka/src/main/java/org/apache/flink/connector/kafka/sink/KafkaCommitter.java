/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.RestoredSubtaskCommittablesTracker;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Committer implementation for {@link KafkaSink}
 *
 * <p>The committer is responsible to finalize the Kafka transactions by committing them.
 */
class KafkaCommitter
        implements RestoredSubtaskCommittablesTracker<KafkaCommittable>,
                Committer<KafkaCommittable>,
                Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommitter.class);
    public static final String UNKNOWN_PRODUCER_ID_ERROR_MESSAGE =
            "because of a bug in the Kafka broker (KAFKA-9310). Please upgrade to Kafka 2.5+. If you are running with concurrent checkpoints, you also may want to try without them.\n"
                    + "To avoid data loss, the application will restart.";

    private final Properties kafkaProducerConfig;

    private PreviousActiveCommittableRangeAborter previousActiveCommittableRangeAborter;

    KafkaCommitter(Properties kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    public void preCommitRestoredSubtaskCommittables(Set<Integer> restoredSubtaskIds) {
        if (!restoredSubtaskIds.isEmpty()) {
            previousActiveCommittableRangeAborter =
                    new PreviousActiveCommittableRangeAborter(
                            restoredSubtaskIds,
                            committableToAbort -> {
                                try (final TwoPhaseCommitProducer<?, ?> abortingProducer =
                                        new TwoPhaseCommitProducer<>(
                                                kafkaProducerConfig, committableToAbort)) {
                                    LOG.info(
                                            "Aborting potential lingering transaction {} that was in previous execution's active committable range.",
                                            committableToAbort);
                                    abortingProducer.initAndAbortOngoingTransaction();
                                }
                            });
        }
    }

    public void postCommitRestoredSubtaskCommittables() {
        if (previousActiveCommittableRangeAborter != null) {
            previousActiveCommittableRangeAborter
                    .abortNonCheckpointedCommittablesWithinActiveRange();
            // dereference for GC; this will no longer be needed for rest of the execution
            previousActiveCommittableRangeAborter = null;
        }
    }

    @Override
    public void commit(Collection<CommitRequest<KafkaCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<KafkaCommittable> request : requests) {
            final KafkaCommittable committable = request.getCommittable();
            final String transactionalId = committable.getTransactionalId();

            // if this is a commit for restored committables, track it
            if (committable.getVersion() == ConfluentKafkaCommittableV1.VERSION
                    && previousActiveCommittableRangeAborter != null) {
                previousActiveCommittableRangeAborter.trackRestoredCommittable(
                        ConfluentKafkaCommittableV1.tryCast(committable));
            }

            LOG.debug("Committing Kafka transaction {}", transactionalId);
            Optional<Recyclable<? extends InternalKafkaProducer<?, ?>>> recyclable =
                    committable.getProducer();
            // if there isn't a recyclable producer bundled with the committable,
            // then this means this committable was a restored one instead of a
            // fresh committable being passed from the writer operator
            final boolean isRecoveredCommittable = !recyclable.isPresent();
            InternalKafkaProducer<?, ?> producer = null;
            try {
                if (isRecoveredCommittable) {
                    producer = getRecoveryProducer(committable);
                    producer.resumePreparedTransaction(committable);
                } else {
                    producer = recyclable.get().getObject();
                }
                producer.commitTransaction();
                producer.flush();
                if (isRecoveredCommittable) {
                    producer.close();
                } else {
                    recyclable.get().close();
                }
            } catch (RetriableException e) {
                LOG.warn(
                        "Encountered retriable exception while committing {}.", transactionalId, e);
                request.retryLater();
                if (isRecoveredCommittable && producer != null) {
                    producer.close();
                }
            } catch (ProducerFencedException e) {
                // initTransaction has been called on this transaction before
                LOG.error(
                        "Unable to commit transaction ({}) because its producer is already fenced."
                                + " This means that you either have a different producer with the same '{}' (this is"
                                + " unlikely with the '{}' as all generated ids are unique and shouldn't be reused)"
                                + " or recovery took longer than '{}' ({}ms). In both cases this most likely signals data loss,"
                                + " please consult the Flink documentation for more details.",
                        request,
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                        KafkaSink.class.getSimpleName(),
                        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                        kafkaProducerConfig.getProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG),
                        e);
                if (isRecoveredCommittable) {
                    if (producer != null) {
                        producer.close();
                    }
                } else {
                    recyclable.get().close();
                }
                request.signalFailedWithKnownReason(e);
            } catch (InvalidTxnStateException e) {
                // This exception only occurs when aborting after a commit or vice versa.
                // It does not appear on double commits or double aborts.
                LOG.error(
                        "Unable to commit transaction ({}) because it's in an invalid state. "
                                + "Most likely the transaction has been aborted for some reason. Please check the Kafka logs for more details.",
                        request,
                        e);
                if (isRecoveredCommittable) {
                    if (producer != null) {
                        producer.close();
                    }
                } else {
                    recyclable.get().close();
                }
                request.signalFailedWithKnownReason(e);
            } catch (UnknownProducerIdException e) {
                LOG.error(
                        "Unable to commit transaction ({}) " + UNKNOWN_PRODUCER_ID_ERROR_MESSAGE,
                        request,
                        e);
                if (isRecoveredCommittable) {
                    if (producer != null) {
                        producer.close();
                    }
                } else {
                    recyclable.get().close();
                }
                request.signalFailedWithKnownReason(e);
            } catch (Exception e) {
                LOG.error(
                        "Transaction ({}) encountered error and data has been potentially lost.",
                        request,
                        e);
                if (isRecoveredCommittable) {
                    if (producer != null) {
                        producer.close();
                    }
                } else {
                    recyclable.get().close();
                }
                request.signalFailedWithUnknownReason(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        previousActiveCommittableRangeAborter = null;
    }

    /**
     * Creates a producer that can commit into the same transaction as the upstream producer that
     * was serialized into {@link KafkaCommittable}.
     */
    private InternalKafkaProducer<?, ?> getRecoveryProducer(KafkaCommittable committable) {
        InternalKafkaProducer<?, ?> recoveryProducer;
        switch (committable.getVersion()) {
            case ConfluentKafkaCommittableV1.VERSION:
                recoveryProducer =
                        new TwoPhaseCommitProducer<>(
                                kafkaProducerConfig, (ConfluentKafkaCommittableV1) committable);
                break;
            case KafkaCommittableV1.VERSION:
                recoveryProducer =
                        new FlinkKafkaInternalProducer<>(
                                kafkaProducerConfig, committable.getTransactionalId());
                break;
            default:
                throw new RuntimeException("Unexpected KafkaCommittable version: " + committable);
        }
        return recoveryProducer;
    }
}
