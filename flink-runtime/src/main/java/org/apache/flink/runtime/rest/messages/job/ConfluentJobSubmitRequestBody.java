/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** Request for submitting a confluent job. */
@Confluent
public final class ConfluentJobSubmitRequestBody implements RequestBody {
    public static final String FIELD_NAME_JOB_ID = "jobId";
    public static final String FIELD_NAME_JOB_SAVEPOINT = "savepointPath";
    public static final String FIELD_NAME_JOB_ARGUMENTS = "generatorArguments";
    public static final String FIELD_NAME_JOB_CONFIGURATION = "configuration";

    @JsonProperty(FIELD_NAME_JOB_ID)
    @Nullable
    public final String jobId;

    @JsonProperty(FIELD_NAME_JOB_SAVEPOINT)
    @Nullable
    public final String savepointPath;

    @JsonProperty(FIELD_NAME_JOB_ARGUMENTS)
    @Nonnull
    public final Collection<String> generatorArguments;

    @JsonProperty(FIELD_NAME_JOB_CONFIGURATION)
    @Nonnull
    public final Map<String, String> configuration;

    @JsonCreator
    public ConfluentJobSubmitRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_JOB_ID) String jobId,
            @Nullable @JsonProperty(FIELD_NAME_JOB_SAVEPOINT) String savepointPath,
            @Nullable @JsonProperty(FIELD_NAME_JOB_ARGUMENTS) Collection<String> generatorArguments,
            @Nullable @JsonProperty(FIELD_NAME_JOB_CONFIGURATION)
                    Map<String, String> configuration) {
        this.jobId = jobId;
        this.generatorArguments =
                Optional.ofNullable(generatorArguments).orElse(Collections.emptyList());
        this.savepointPath = savepointPath;
        this.configuration = Optional.ofNullable(configuration).orElse(Collections.emptyMap());
    }
}
