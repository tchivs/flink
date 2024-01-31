/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;

import cloud.confluent.cc_auth_dataplane.v1.AuthDataplaneServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.flink.credentials.KafkaCredentialsOptions.AUTH_DATAPLANE_ENABLED;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.AUTH_DATAPLANE_SERVICE_DEADLINE_MS;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.AUTH_DATAPLANE_SERVICE_HOST;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.AUTH_DATAPLANE_SERVICE_PORT;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.AUTH_SERVICE_SERVER;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.TOKEN_EXCHANGE_TIMEOUT_MS;

/** Utility class common to token exchangers. */
@Confluent
public class TokenExchangerUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TokenExchangerUtil.class);

    public static Optional<String> getServiceAccountPrincipal(
            JobCredentialsMetadata jobCredentialsMetadata) {
        return filterByPrefix(jobCredentialsMetadata.getPrincipals(), "sa-").findFirst();
    }

    public static Optional<String> getUserPrincipal(JobCredentialsMetadata jobCredentialsMetadata) {
        return filterByPrefix(jobCredentialsMetadata.getPrincipals(), "u-").findFirst();
    }

    public static List<String> getIdentityPoolPrincipals(
            JobCredentialsMetadata jobCredentialsMetadata) {
        List<String> identityPools =
                filterByPrefix(jobCredentialsMetadata.getPrincipals(), "pool-")
                        .collect(Collectors.toList());

        // Required for users, who will pass in a list containing user resource id + group mappings
        // Group mappings are a special type of identity pool that are prefixed instead by `group-`
        // See
        // https://confluentinc.atlassian.net/wiki/spaces/SECENG/pages/3168567337/Instances+of+manual+pool-+prefix+checking#Dependencies
        List<String> groupMappings =
                filterByPrefix(jobCredentialsMetadata.getPrincipals(), "group-")
                        .collect(Collectors.toList());

        identityPools.addAll(groupMappings);
        return identityPools;
    }

    public static TokenExchanger createTokenExchanger(Configuration configuration) {
        if (configuration.getBoolean(AUTH_DATAPLANE_ENABLED)) {
            LOG.info("Using dataplane auth token exchanger.");
            Channel authDataplaneChannel =
                    ManagedChannelBuilder.forAddress(
                                    configuration.getString(AUTH_DATAPLANE_SERVICE_HOST),
                                    configuration.getInteger(AUTH_DATAPLANE_SERVICE_PORT))
                            .usePlaintext()
                            .build();
            AuthDataplaneServiceGrpc.AuthDataplaneServiceBlockingStub authDataplaneService =
                    AuthDataplaneServiceGrpc.newBlockingStub(authDataplaneChannel);
            return new DataplaneTokenExchanger(
                    authDataplaneService,
                    configuration.getLong(AUTH_DATAPLANE_SERVICE_DEADLINE_MS));
        } else {
            LOG.info("Using controlplane auth token exchanger.");
            return new TokenExchangerImpl(
                    configuration.getString(AUTH_SERVICE_SERVER),
                    configuration.getLong(TOKEN_EXCHANGE_TIMEOUT_MS));
        }
    }

    private static Stream<String> filterByPrefix(List<String> principals, String prefix) {
        return principals.stream().filter(principal -> principal.startsWith(prefix));
    }
}
