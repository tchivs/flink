/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** Metadata passed along with a submitted job graph. */
@Confluent
public class JobCredentialsMetadata implements Serializable {

    private static final long serialVersionUID = 4229894986752990002L;

    private final JobID jobID;
    private final String statementIdCRN;
    private final String computePoolId;
    private final String identityPoolId;
    private final List<String> principals;
    private final long startTimeMs;
    private final long tokenUpdateTimeMs;

    public JobCredentialsMetadata(
            JobID jobID,
            String statementIdCRN,
            String computePoolId,
            String identityPoolId,
            List<String> principals,
            long startTimeMs,
            long tokenUpdateTimeMs) {
        this.jobID = jobID;
        this.statementIdCRN = statementIdCRN;
        this.computePoolId = computePoolId;
        this.identityPoolId = identityPoolId;
        this.startTimeMs = startTimeMs;
        this.tokenUpdateTimeMs = tokenUpdateTimeMs;
        this.principals = principals != null ? principals : Collections.emptyList();
    }

    public String getStatementIdCRN() {
        return statementIdCRN;
    }

    public String getIdentityPoolId() {
        return identityPoolId;
    }

    public String getComputePoolId() {
        return computePoolId;
    }

    public JobID getJobID() {
        return jobID;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getTokenUpdateTimeMs() {
        return tokenUpdateTimeMs;
    }

    public List<String> getPrincipals() {
        return principals;
    }

    public JobCredentialsMetadata withNewTokenUpdateTime(long tokenUpdateTimeMs) {
        return new JobCredentialsMetadata(
                jobID,
                statementIdCRN,
                computePoolId,
                identityPoolId,
                principals,
                startTimeMs,
                tokenUpdateTimeMs);
    }
}
