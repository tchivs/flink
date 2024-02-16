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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link AbstractS3DelegationTokenProvider}. */
public class AbstractS3DelegationTokenProviderTest {

    private static final String REGION = "testRegion";
    private static final String ACCESS_KEY_ID = "testAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "testSecretAccessKey";

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

        assertEquals(provider.delegationTokensRequired(), expectedResult);
    }

    public static Stream<Arguments> delegationTokensRequiredArguments() {
        return Stream.of(
                Arguments.of(null, null, null, null, false),
                Arguments.of(null, null, null, true, false),
                Arguments.of(null, null, null, false, true),
                Arguments.of(REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY, null, true),
                Arguments.of(REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY, true, true),
                Arguments.of(REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY, false, true),
                Arguments.of(REGION, ACCESS_KEY_ID, null, null, false),
                Arguments.of(REGION, ACCESS_KEY_ID, null, true, false),
                Arguments.of(REGION, ACCESS_KEY_ID, null, false, true),
                Arguments.of(REGION, null, SECRET_ACCESS_KEY, null, false),
                Arguments.of(REGION, null, SECRET_ACCESS_KEY, true, false),
                Arguments.of(REGION, null, SECRET_ACCESS_KEY, false, true),
                Arguments.of(REGION, null, null, null, false),
                Arguments.of(REGION, null, null, true, false),
                Arguments.of(REGION, null, null, false, true),
                Arguments.of(null, null, SECRET_ACCESS_KEY, null, false),
                Arguments.of(null, null, SECRET_ACCESS_KEY, true, false),
                Arguments.of(null, null, SECRET_ACCESS_KEY, false, true));
    }
}
