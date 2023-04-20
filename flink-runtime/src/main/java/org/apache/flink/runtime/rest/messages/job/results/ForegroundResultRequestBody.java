/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.job.results;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Optional;

/** {@link RequestBody} to retrieve foreground query results. */
@Confluent
public class ForegroundResultRequestBody implements RequestBody {

    public static final String FIELD_NAME_VERSION = "version";
    public static final String FIELD_NAME_OFFSET = "offset";

    @JsonProperty(FIELD_NAME_VERSION)
    @Nullable
    private final String version;

    @JsonProperty(FIELD_NAME_OFFSET)
    @Nullable
    private final Long offset;

    @JsonCreator
    public ForegroundResultRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_VERSION) final String version,
            @Nullable @JsonProperty(FIELD_NAME_OFFSET) final Long offset) {
        this.version = version;
        this.offset = offset;
    }

    @JsonIgnore
    public Optional<String> getVersion() {
        return Optional.ofNullable(version);
    }

    @JsonIgnore
    public Optional<Long> getOffset() {
        return Optional.ofNullable(offset);
    }
}
