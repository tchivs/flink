/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import io.confluent.flink.jobgraph.v2.FlinkClientWrapperV2;

import java.io.Closeable;

/** An abstraction around Flink Client. The purpose is to encapsulate Flink Client version. */
@Deprecated
public interface FlinkClientWrapper extends FlinkClientWrapperV2, Closeable {}
