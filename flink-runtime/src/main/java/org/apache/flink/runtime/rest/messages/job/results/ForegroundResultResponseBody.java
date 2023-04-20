/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.job.results;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonRawValue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/** {@link ResponseBody} to retrieve foreground query results. */
@Confluent
public class ForegroundResultResponseBody implements ResponseBody {

    public static final String FIELD_NAME_VERSION = "version";
    public static final String FIELD_NAME_LAST_CHECKPOINTED_OFFSET = "lastCheckpointedOffset";
    public static final String FIELD_NAME_DATA = "data";

    @JsonProperty(FIELD_NAME_VERSION)
    private String version;

    @JsonProperty(FIELD_NAME_LAST_CHECKPOINTED_OFFSET)
    private Long lastCheckpointedOffset;

    @JsonProperty(FIELD_NAME_DATA)
    @JsonRawValue
    private String data;

    @JsonCreator
    public ForegroundResultResponseBody(
            @JsonProperty(FIELD_NAME_VERSION) String version,
            @JsonProperty(FIELD_NAME_LAST_CHECKPOINTED_OFFSET) Long lastCheckpointedOffset,
            @JsonProperty(FIELD_NAME_DATA) String data) {
        this.version = version;
        this.lastCheckpointedOffset = lastCheckpointedOffset;
        this.data = data;
    }

    @JsonIgnore
    public static ForegroundResultResponseBody of(
            String version, Long lastCheckpointedOffset, List<byte[]> data) {
        return new ForegroundResultResponseBody(version, lastCheckpointedOffset, formatData(data));
    }

    @JsonIgnore
    public String getVersion() {
        return version;
    }

    @JsonIgnore
    public Long getLastCheckpointedOffset() {
        return lastCheckpointedOffset;
    }

    @JsonIgnore
    public String getData() {
        return data;
    }

    private static String formatData(List<byte[]> data) {
        return data.stream()
                .map(row -> new String(row, StandardCharsets.UTF_8))
                .collect(Collectors.joining(",", "[", "]"));
    }
}
