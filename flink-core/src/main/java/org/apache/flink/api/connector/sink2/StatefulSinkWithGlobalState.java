/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * A {@link Sink} with stateful {@link SinkWriter}s and also share a global state.
 *
 * <p>The {@link StatefulSink} needs to be serializable. All configuration should be validated
 * eagerly. The respective sink writers are transient and will only be created in the subtasks on
 * the taskmanagers.
 */
@Confluent
@PublicEvolving
public interface StatefulSinkWithGlobalState<InputT, WriterStateT, GlobalStateT>
        extends Sink<InputT> {

    /**
     * Create a {@link StatefulSink.StatefulSinkWriter}.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    StatefulSinkWriter<InputT, WriterStateT, GlobalStateT> createWriter(InitContext context)
            throws IOException;

    /**
     * Create a {@link StatefulSink.StatefulSinkWriter} from a recovered state.
     *
     * @param context the runtime context.
     * @param recoveredWriterState recovered writer states.
     * @param recoveredGlobalState recovered global state.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    StatefulSinkWriter<InputT, WriterStateT, GlobalStateT> restoreWriter(
            InitContext context,
            Collection<WriterStateT> recoveredWriterState,
            GlobalStateT recoveredGlobalState)
            throws IOException;

    /**
     * Any stateful sink needs to provide this state serializer and implement {@link
     * StatefulSinkWriter#snapshotState(long)} properly. The respective state is used in {@link
     * #restoreWriter(InitContext, Collection, GlobalStateT)} on recovery.
     *
     * @return the serializer of the writer's state type.
     */
    SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer();

    /**
     * Any stateful sink needs to provide this state serializer and implement {@link
     * StatefulSinkWriter#snapshotGlobalState(long)} properly. The respective state is used in
     * {@link #restoreWriter(InitContext, Collection, GlobalStateT)} on recovery.
     *
     * @return the serializer of the global state type.
     */
    SimpleVersionedSerializer<GlobalStateT> getGlobalStateSerializer();

    /**
     * A {@link SinkWriter} whose state needs to be checkpointed.
     *
     * @param <InputT> The type of the sink writer's input
     * @param <WriterStateT> The type of the writer's state
     * @param <GlobalStateT>> the type of the global state
     */
    @PublicEvolving
    interface StatefulSinkWriter<InputT, WriterStateT, GlobalStateT> extends SinkWriter<InputT> {
        /**
         * @return The writer's state.
         * @throws IOException if fail to snapshot writer's state.
         */
        List<WriterStateT> snapshotState(long checkpointId) throws IOException;

        GlobalStateT snapshotGlobalState(long checkpointId) throws IOException;
    }
}
