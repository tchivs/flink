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

package org.apache.flink.fs.s3.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

/** Tests that the file system factory picks up the entropy configuration properly. */
public class S3EntropyFsFactoryTest extends TestLogger {

    @Test
    public void testEntropyInjectionConfig() throws Exception {
        final Configuration conf = new Configuration();
        conf.setString("s3.entropy.key", "__entropy__");
        conf.setInteger("s3.entropy.length", 7);

        TestS3FileSystemFactory factory = new TestS3FileSystemFactory();
        factory.configure(conf);

        FlinkS3FileSystem fs = (FlinkS3FileSystem) factory.create(new URI("s3://test"));
        assertEquals("__entropy__", fs.getEntropyInjectionKey());
        assertEquals(7, fs.generateEntropy().length());
    }

    /**
     * Test validates that the produced by AbstractS3FileSystemFactory object will contains only
     * first path from multiple paths in config.
     */
    @Test
    public void testMultipleTempDirsConfig() throws Exception {
        final Configuration conf = new Configuration();
        String dir1 = "/tmp/dir1";
        String dir2 = "/tmp/dir2";
        conf.setString("io.tmp.dirs", dir1 + "," + dir2);

        TestS3FileSystemFactory factory = new TestS3FileSystemFactory();
        factory.configure(conf);

        FlinkS3FileSystem fs = (FlinkS3FileSystem) factory.create(new URI("s3://test"));
        assertEquals(fs.getLocalTmpDir(), dir1);
    }
}
