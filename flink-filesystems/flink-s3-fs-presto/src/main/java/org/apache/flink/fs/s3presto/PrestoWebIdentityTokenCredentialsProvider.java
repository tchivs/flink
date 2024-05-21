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

package org.apache.flink.fs.s3presto;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

/**
 * Adapter for {@link WebIdentityTokenCredentialsProvider} with the correct constructor for presto
 * S3 factory.
 */
public class PrestoWebIdentityTokenCredentialsProvider implements AWSCredentialsProvider {

    private final AWSCredentialsProvider delegate = new WebIdentityTokenCredentialsProvider();

    public PrestoWebIdentityTokenCredentialsProvider(URI uri, Configuration configuration) {
        // no-op.
    }

    @Override
    public AWSCredentials getCredentials() {
        return delegate.getCredentials();
    }

    @Override
    public void refresh() {
        delegate.refresh();
    }
}
