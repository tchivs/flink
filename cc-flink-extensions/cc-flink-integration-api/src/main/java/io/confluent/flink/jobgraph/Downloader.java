/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

/**
 * A downloader that can be used to download {@link JobGraphGenerator} argument data, e.g.
 * CompiledPlan.
 */
@Deprecated
public interface Downloader {
    void download(String sourceUrl, String localDestination);
}
