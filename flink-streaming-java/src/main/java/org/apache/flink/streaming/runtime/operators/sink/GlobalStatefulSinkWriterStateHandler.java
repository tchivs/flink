/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWithGlobalState;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.util.CollectionUtil;

import java.util.Collections;
import java.util.List;

/** A {@link SinkWriterStateHandler} for {@link StatefulSinkWithGlobalState}. */
@Confluent
@Internal
public class GlobalStatefulSinkWriterStateHandler<InputT, WriterStateT, GlobalStateT>
        implements SinkWriterStateHandler<InputT> {

    /** The writer's state descriptor. */
    private static final ListStateDescriptor<byte[]> WRITER_RAW_STATES_DESC =
            new ListStateDescriptor<>("writer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    /** The global state's state descriptor. */
    private static final ListStateDescriptor<byte[]> GLOBAL_RAW_STATE_DESC =
            new ListStateDescriptor<>("global_raw_state", BytePrimitiveArraySerializer.INSTANCE);

    /** Serializer for writer state. */
    private final SimpleVersionedSerializer<WriterStateT> writerStateSerializer;

    /** Serializer for global state. */
    private final SimpleVersionedSerializer<GlobalStateT> globalStateSerializer;

    private final StatefulSinkWithGlobalState<InputT, WriterStateT, GlobalStateT> sink;

    // ------------------------------- runtime fields ---------------------------------------

    private ListState<WriterStateT> writerState;
    private ListState<GlobalStateT> globalState;
    private StatefulSinkWithGlobalState.StatefulSinkWriter<InputT, WriterStateT, GlobalStateT>
            sinkWriter;
    private int subtaskIndex;

    public GlobalStatefulSinkWriterStateHandler(
            StatefulSinkWithGlobalState<InputT, WriterStateT, GlobalStateT> sink) {
        this.sink = sink;
        this.writerStateSerializer = sink.getWriterStateSerializer();
        this.globalStateSerializer = sink.getGlobalStateSerializer();
    }

    @Override
    public SinkWriter<InputT> createWriter(
            Sink.InitContext initContext, StateInitializationContext context) throws Exception {
        subtaskIndex = initContext.getSubtaskId();

        final ListState<byte[]> writerRawState =
                context.getOperatorStateStore().getListState(WRITER_RAW_STATES_DESC);
        final ListState<byte[]> globalRawState =
                context.getOperatorStateStore().getUnionListState(GLOBAL_RAW_STATE_DESC);

        writerState = new SimpleVersionedListState<>(writerRawState, writerStateSerializer);
        globalState = new SimpleVersionedListState<>(globalRawState, globalStateSerializer);

        if (!context.isRestored()) {
            sinkWriter = sink.createWriter(initContext);
            return sinkWriter;
        }

        // restore writer with writer states + global state
        final List<WriterStateT> deserializedWriterStates =
                CollectionUtil.iterableToList(writerState.get());
        final GlobalStateT deserializedGlobalState =
                CollectionUtil.iterableToList(globalState.get()).get(0);
        sinkWriter =
                sink.restoreWriter(initContext, deserializedWriterStates, deserializedGlobalState);

        // since we're using union list state, it is critical to clear the state after restore
        // to prevent infinite state size increase on every restore. The global state will be
        // recreated
        // by first subtask on every snapshot.
        globalState.clear();

        return this.sinkWriter;
    }

    @Override
    public void snapshotState(long checkpointId) throws Exception {
        writerState.update(sinkWriter.snapshotState(checkpointId));

        // use the first subtask as the leader for snapshotting global state
        if (subtaskIndex == 0) {
            globalState.update(
                    Collections.singletonList(sinkWriter.snapshotGlobalState(checkpointId)));
        }
    }
}
