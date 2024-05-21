/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.util.FlinkRuntimeException;

import cloud.confluent.cc_auth_dataplane.v1.AuthDataplaneServiceGrpc.AuthDataplaneServiceBlockingStub;
import cloud.confluent.cc_auth_dataplane.v1.IssueFlinkAuthTokenRequest;
import cloud.confluent.cc_auth_dataplane.v1.IssueFlinkAuthTokenResponse;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.confluent.flink.credentials.TokenExchangerUtil.getIdentityPoolPrincipals;
import static io.confluent.flink.credentials.TokenExchangerUtil.getServiceAccountPrincipal;
import static io.confluent.flink.credentials.TokenExchangerUtil.getUserPrincipal;

/** Uses the data plane auth service to do token exchanges. */
@Confluent
public class DataplaneTokenExchanger implements TokenExchanger {
    private static final Logger LOG = LoggerFactory.getLogger(DataplaneTokenExchanger.class);

    private final AuthDataplaneServiceBlockingStub authDataplaneService;
    private final long deadlineMs;

    public DataplaneTokenExchanger(
            AuthDataplaneServiceBlockingStub authDataplaneService, long deadlineMs) {
        this.authDataplaneService = authDataplaneService;
        this.deadlineMs = deadlineMs;
    }

    @Override
    public DPATTokens fetch(
            Pair<String, String> staticCredentials, JobCredentialsMetadata jobCredentialsMetadata) {

        Optional<String> serviceAccount = getServiceAccountPrincipal(jobCredentialsMetadata);
        Optional<String> user = getUserPrincipal(jobCredentialsMetadata);
        List<String> identityPools = getIdentityPoolPrincipals(jobCredentialsMetadata);

        IssueFlinkAuthTokenRequest.Builder request =
                IssueFlinkAuthTokenRequest.newBuilder()
                        .setApiKey(staticCredentials.getKey())
                        .setApiSecret(staticCredentials.getValue())
                        .setComputePoolId(jobCredentialsMetadata.getComputePoolId())
                        .setStatementCrn(jobCredentialsMetadata.getStatementIdCRN());

        if (serviceAccount.isPresent()) {
            request.setServiceAccountId(serviceAccount.get());
        } else if (user.isPresent()) {
            request.setUserResourceId(user.get());
            if (!identityPools.isEmpty()) {
                request.addAllIdentityPoolIds(identityPools);
            }
        } else {
            throw new FlinkRuntimeException("Neither service account nor user is set");
        }

        try {
            IssueFlinkAuthTokenResponse response =
                    authDataplaneService
                            .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                            .issueFlinkAuthToken(request.build());

            // If the job contains UDFs, we need to fetch a separate token for that use.
            Optional<String> udfToken = Optional.empty();
            if (jobCredentialsMetadata.jobContainsUDFs()) {
                LOG.info("Doing second request for UDF token (dataplane)");
                request.setTarget("cc-secure-compute-gateway");

                IssueFlinkAuthTokenResponse udfResponse =
                        authDataplaneService
                                .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                                .issueFlinkAuthToken(request.build());
                udfToken = Optional.of(udfResponse.getToken());
            }

            return new DPATTokens(response.getToken(), udfToken);
        } catch (Throwable t) {
            LOG.error("Failed to fetch token (dataplane)", t);
            throw new FlinkRuntimeException("Failed to fetch token (dataplane)", t);
        }
    }
}
