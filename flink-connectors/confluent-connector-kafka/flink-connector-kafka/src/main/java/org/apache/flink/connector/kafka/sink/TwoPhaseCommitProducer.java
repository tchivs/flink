/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link InternalKafkaProducer} that is implemented using Confluent's internal implementation for
 * the KIP-939 Kafka client.
 */
@Confluent
@Internal
public class TwoPhaseCommitProducer<K, V> extends KafkaProducer<K, V>
        implements InternalKafkaProducer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitProducer.class);

    @Nullable private final ConfluentKafkaCommittableV1 assignedCommittable;

    private volatile boolean inTransaction = false;
    private volatile boolean hasRecordsInTransaction = false;
    private volatile boolean closed = false;

    public TwoPhaseCommitProducer(
            Properties properties, @Nullable ConfluentKafkaCommittableV1 assignedCommittable) {
        super(withTransactionalId(properties, assignedCommittable));
        this.assignedCommittable = assignedCommittable;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        if (inTransaction) {
            hasRecordsInTransaction = true;
        }
        return super.send(record, callback);
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        super.beginTransaction();
        inTransaction = true;
    }

    @Override
    public KafkaCommittable prepareTransaction(Consumer<InternalKafkaProducer<K, V>> recycler) {
        // TODO: the public version of KIP-939 will have an explicit prepare call; call that as well
        return ConfluentKafkaCommittableV1.applyRecyclableProducer(
                assignedCommittable, new Recyclable<>(this, recycler));
    }

    @Override
    public boolean initAndAbortOngoingTransaction() {
        super.initTransactions(false);
        return currentProducerIdAndEpoch().epoch != 0;
    }

    @Override
    public void resumePreparedTransaction(KafkaCommittable restoredCommittable) {
        super.initTransactions(true);
        beginTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        LOG.debug("abortTransaction {}", assignedCommittable.getTransactionalId());
        checkState(inTransaction, "Transaction was not started");
        inTransaction = false;
        hasRecordsInTransaction = false;
        super.abortTransaction();
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        LOG.debug("commitTransaction {}", assignedCommittable.getTransactionalId());
        checkState(inTransaction, "Transaction was not started");
        inTransaction = false;
        hasRecordsInTransaction = false;
        super.commitTransaction();
    }

    @Override
    public boolean isInTransaction() {
        return inTransaction;
    }

    @Override
    public boolean hasRecordsInTransaction() {
        return hasRecordsInTransaction;
    }

    @Override
    @Nullable
    public ConfluentKafkaCommittableV1 getAssignedCommittable() {
        return assignedCommittable;
    }

    @Override
    public void close() {
        closed = true;
        if (inTransaction) {
            // closing with zero duration prevents ongoing transaction from being auto-aborted
            super.close(Duration.ZERO);
        } else {
            super.close(Duration.ofHours(1));
        }
    }

    @Override
    public void close(Duration timeout) {
        closed = true;
        super.close(timeout);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private static Properties withTransactionalId(
            Properties properties, @Nullable ConfluentKafkaCommittableV1 assignedCommittable) {
        if (assignedCommittable == null) {
            return properties;
        }
        Properties props = new Properties();
        props.putAll(properties);
        props.setProperty(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, assignedCommittable.getTransactionalId());
        props.setProperty(ProducerConfig.ENABLE_TWO_PHASE_COMMIT_CONFIG, "true");
        return props;
    }
}
