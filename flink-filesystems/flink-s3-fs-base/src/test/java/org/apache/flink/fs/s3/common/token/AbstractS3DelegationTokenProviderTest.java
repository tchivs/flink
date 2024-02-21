/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.s3.common.token;

import org.apache.flink.configuration.Configuration;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.util.EC2MetadataUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AbstractS3DelegationTokenProvider}. */
@Testcontainers
public class AbstractS3DelegationTokenProviderTest {

    private static final String REGION = "testRegion";
    private static final String ACCESS_KEY_ID = "testAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "testSecretAccessKey";

    @Container
    public static final GenericContainer<?> EC2_METADATA =
            new GenericContainer<>("public.ecr.aws/aws-ec2/amazon-ec2-metadata-mock:v1.11.2")
                    .withExposedPorts(1338);

    private AbstractS3DelegationTokenProvider provider;

    @BeforeEach
    public void beforeEach() {
        provider =
                new AbstractS3DelegationTokenProvider() {
                    @Override
                    public String serviceName() {
                        return "s3";
                    }
                };
    }

    @ParameterizedTest
    @MethodSource("delegationTokensRequiredArguments")
    public void delegationTokensRequired(
            String region,
            String accessKey,
            String secretKey,
            Boolean isCredentialsRequired,
            boolean mockAws,
            boolean expectedResult)
            throws Exception {
        try {
            System.setProperty(
                    SDKGlobalConfiguration.EC2_METADATA_SERVICE_OVERRIDE_SYSTEM_PROPERTY,
                    mockAws
                            ? "http://localhost:" + EC2_METADATA.getMappedPort(1338)
                            : "http://unavailable:1234");

            testDelegationTokensRequired(
                    region, accessKey, secretKey, isCredentialsRequired, expectedResult);
        } finally {
            // clear property and cached information in EC2MetadataUtils
            Field cacheField = EC2MetadataUtils.class.getDeclaredField("cache");
            cacheField.setAccessible(true);
            Map<?, ?> cache = (Map<?, ?>) cacheField.get(null);
            cache.clear();
            System.clearProperty(
                    SDKGlobalConfiguration.EC2_METADATA_SERVICE_OVERRIDE_SYSTEM_PROPERTY);
        }
    }

    private void testDelegationTokensRequired(
            String region,
            String accessKey,
            String secretKey,
            Boolean isCredentialsRequired,
            boolean expectedResult) {
        Configuration configuration = new Configuration();

        if (region != null) {
            configuration.setString(CONFIG_PREFIX + ".s3.region", region);
        }
        if (accessKey != null) {
            configuration.setString(CONFIG_PREFIX + ".s3.access-key", accessKey);
        }
        if (secretKey != null) {
            configuration.setString(CONFIG_PREFIX + ".s3.secret-key", secretKey);
        }
        if (isCredentialsRequired != null) {
            configuration.setBoolean(
                    CONFIG_PREFIX + ".s3.credentials-required", isCredentialsRequired);
        }
        provider.init(configuration);

        assertThat(provider.delegationTokensRequired()).isEqualTo(expectedResult);
    }

    public static Stream<Arguments> delegationTokensRequiredArguments() {
        return Stream.of(
                Arguments.of(null, null, null, null, false, false),
                Arguments.of(null, null, null, true, false, false),
                Arguments.of(null, null, null, false, false, false),
                Arguments.of(null, null, null, false, true, true),
                Arguments.of(REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY, null, false, true),
                Arguments.of(REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY, true, false, true),
                Arguments.of(REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY, false, false, false),
                Arguments.of(REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY, false, true, true),
                Arguments.of(REGION, ACCESS_KEY_ID, null, null, false, false),
                Arguments.of(REGION, ACCESS_KEY_ID, null, true, false, false),
                Arguments.of(REGION, ACCESS_KEY_ID, null, false, false, false),
                Arguments.of(REGION, ACCESS_KEY_ID, null, false, true, true),
                Arguments.of(REGION, null, SECRET_ACCESS_KEY, null, false, false),
                Arguments.of(REGION, null, SECRET_ACCESS_KEY, true, false, false),
                Arguments.of(REGION, null, SECRET_ACCESS_KEY, false, false, false),
                Arguments.of(REGION, null, SECRET_ACCESS_KEY, false, true, true),
                Arguments.of(REGION, null, null, null, false, false),
                Arguments.of(REGION, null, null, true, false, false),
                Arguments.of(REGION, null, null, false, false, false),
                Arguments.of(REGION, null, null, false, true, true),
                Arguments.of(null, null, SECRET_ACCESS_KEY, null, false, false),
                Arguments.of(null, null, SECRET_ACCESS_KEY, true, false, false),
                Arguments.of(null, null, SECRET_ACCESS_KEY, false, false, false),
                Arguments.of(null, null, SECRET_ACCESS_KEY, false, true, true));
    }
}
