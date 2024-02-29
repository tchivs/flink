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
import org.apache.flink.core.fs.EntropyInjectingFileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.PathsCopyingFileSystem;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.core.fs.RefCountedTmpFileCreator;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.fs.s3.common.writer.S3RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.ACCESS_KEY;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.ENDPOINT;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_EXTRA_ARGS;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_PATH;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.SECRET_KEY;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystem} interface for S3. This
 * class implements the common behavior implemented directly by Flink and delegates common calls to
 * an implementation of Hadoop's filesystem abstraction.
 *
 * <p>Optionally this {@link FlinkS3FileSystem} can use <a href="https://github.com/peak/s5cmd">the
 * s5cmd tool</a> to speed up copying files.
 */
public class FlinkS3FileSystem extends HadoopFileSystem
        implements EntropyInjectingFileSystem, PathsCopyingFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkS3FileSystem.class);

    @Nullable private final String entropyInjectionKey;

    private final int entropyLength;

    // ------------------- Recoverable Writer Parameters -------------------

    /** The minimum size of a part in the multipart upload, except for the last part: 5 MIBytes. */
    public static final long S3_MULTIPART_MIN_PART_SIZE = 5L << 20;

    private final String localTmpDir;

    private final FunctionWithException<File, RefCountedFileWithStream, IOException> tmpFileCreator;

    @Nullable private final S3AccessHelper s3AccessHelper;

    private final Executor uploadThreadPool;

    private final long s3uploadPartSize;

    private final int maxConcurrentUploadsPerStream;

    @Nullable private final S5CmdConfiguration s5CmdConfiguration;

    /** POJO representing parameters to configure s5cmd. */
    public static class S5CmdConfiguration {
        private final String path;
        private final List<String> args;
        @Nullable private final String accessArtifact;
        @Nullable private final String secretArtifact;
        @Nullable private final String endpoint;

        /** All parameters can be empty. */
        public S5CmdConfiguration(
                String path,
                String args,
                @Nullable String accessArtifact,
                @Nullable String secretArtifact,
                @Nullable String endpoint) {
            if (!path.isEmpty()) {
                File s5CmdFile = new File(path);
                checkArgument(s5CmdFile.isFile(), "Unable to find s5cmd binary under [%s]", path);
                checkArgument(
                        s5CmdFile.canExecute(), "s5cmd binary under [%s] is not executable", path);
            }
            this.path = path;
            this.args = Arrays.asList(args.split("\\s+"));
            this.accessArtifact = accessArtifact;
            this.secretArtifact = secretArtifact;
            this.endpoint = endpoint;
        }

        public static Optional<S5CmdConfiguration> of(Configuration flinkConfig) {
            return flinkConfig
                    .getOptional(S5CMD_PATH)
                    .map(
                            s ->
                                    new S5CmdConfiguration(
                                            s,
                                            flinkConfig.getString(S5CMD_EXTRA_ARGS),
                                            flinkConfig.get(ACCESS_KEY),
                                            flinkConfig.get(SECRET_KEY),
                                            flinkConfig.get(ENDPOINT)));
        }

        private void configureEnvironment(Map<String, String> environment) {
            maybeSetEnvironmentVariable(environment, "AWS_ACCESS_KEY_ID", accessArtifact);
            maybeSetEnvironmentVariable(environment, "AWS_SECRET_ACCESS_KEY", secretArtifact);
            maybeSetEnvironmentVariable(environment, "S3_ENDPOINT_URL", endpoint);
        }

        private static void maybeSetEnvironmentVariable(
                Map<String, String> environment, String key, @Nullable String value) {
            if (value == null) {
                return;
            }
            String oldValue = environment.put(key, value);
            if (oldValue != null) {
                LOG.warn(
                        "FlinkS3FileSystem configuration overwrote environment "
                                + "variable's [{}] old value [{}] with [{}]",
                        key,
                        oldValue,
                        value);
            }
        }

        @Override
        public String toString() {
            return "S5CmdConfiguration{"
                    + "path='"
                    + path
                    + '\''
                    + ", args="
                    + args
                    + ", accessArtifact='"
                    + (accessArtifact == null ? null : "****")
                    + '\''
                    + ", secretArtifact='"
                    + (secretArtifact == null ? null : "****")
                    + '\''
                    + ", endpoint='"
                    + endpoint
                    + '\''
                    + '}';
        }
    }

    /**
     * Creates a FlinkS3FileSystem based on the given Hadoop S3 file system. The given Hadoop file
     * system object is expected to be initialized already.
     *
     * <p>This constructor additionally configures the entropy injection for the file system.
     *
     * @param hadoopS3FileSystem The Hadoop FileSystem that will be used under the hood.
     * @param s5CmdConfiguration Configuration of the s5cmd.
     * @param entropyInjectionKey The substring that will be replaced by entropy or removed.
     * @param entropyLength The number of random alphanumeric characters to inject as entropy.
     */
    public FlinkS3FileSystem(
            FileSystem hadoopS3FileSystem,
            @Nullable S5CmdConfiguration s5CmdConfiguration,
            String localTmpDirectory,
            @Nullable String entropyInjectionKey,
            int entropyLength,
            @Nullable S3AccessHelper s3UploadHelper,
            long s3uploadPartSize,
            int maxConcurrentUploadsPerStream) {

        super(hadoopS3FileSystem);

        this.s5CmdConfiguration = s5CmdConfiguration;

        if (entropyInjectionKey != null && entropyLength <= 0) {
            throw new IllegalArgumentException(
                    "Entropy length must be >= 0 when entropy injection key is set");
        }

        this.entropyInjectionKey = entropyInjectionKey;
        this.entropyLength = entropyLength;

        // recoverable writer parameter configuration initialization
        this.localTmpDir = Preconditions.checkNotNull(localTmpDirectory);
        this.tmpFileCreator = RefCountedTmpFileCreator.inDirectories(new File(localTmpDirectory));
        this.s3AccessHelper = s3UploadHelper;
        this.uploadThreadPool = Executors.newCachedThreadPool();

        checkArgument(s3uploadPartSize >= S3_MULTIPART_MIN_PART_SIZE);
        this.s3uploadPartSize = s3uploadPartSize;
        this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;
        LOG.info("Created Flink S3 FS, s5Cmd configuration: {}", s5CmdConfiguration);
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean canCopyPaths() {
        return s5CmdConfiguration != null;
    }

    @Override
    public void copyFiles(List<CopyTask> copyTasks) throws IOException {
        checkState(canCopyPaths(), "#downloadFiles has been called illegally");
        List<String> artefacts = new ArrayList<>();
        artefacts.add(s5CmdConfiguration.path);
        artefacts.addAll(s5CmdConfiguration.args);
        artefacts.add("run");
        castSpell(convertToSpells(copyTasks).iterator(), artefacts.toArray(new String[0]));
    }

    private List<String> convertToSpells(List<CopyTask> copyTasks) throws IOException {
        List<String> spells = new ArrayList<>();
        for (CopyTask copyTask : copyTasks) {
            Files.createDirectories(Paths.get(copyTask.getDestPath().toUri()).getParent());
            spells.add(
                    String.format(
                            "cp %s %s",
                            copyTask.getSrcPath().toUri().toString(),
                            copyTask.getDestPath().getPath()));
        }
        return spells;
    }

    private void castSpell(Iterator<String> spells, String... artefacts) throws IOException {
        LOG.info("Casting spell: {}", Arrays.toString(artefacts));
        StringBuilder stdOutputContent = new StringBuilder();
        int exitCode = 0;
        try {
            ProcessBuilder hogwart = new ProcessBuilder(artefacts);
            s5CmdConfiguration.configureEnvironment(hogwart.environment());
            Process wizard = hogwart.redirectErrorStream(true).start();

            try (BufferedReader stdOutput =
                    new BufferedReader(new InputStreamReader(wizard.getInputStream()))) {

                String stdOutLine;
                @Nullable IOException firstException = null;
                try (BufferedWriter stdIn =
                        new BufferedWriter(new OutputStreamWriter(wizard.getOutputStream()))) {
                    while (spells.hasNext()) {
                        stdIn.write(spells.next());
                        stdIn.newLine();
                        while (stdOutput.ready()) {
                            stdOutLine = stdOutput.readLine();
                            if (stdOutLine != null) {
                                stdOutputContent.append(stdOutLine);
                            }
                        }
                    }
                } catch (IOException e) {
                    firstException = e;
                }

                // Try to read output even in case of an exception to get a better error message.
                // Before reading output we also have to first send EOF on the stdIn, otherwise a
                // deadlock could potentially happen.
                try {
                    while ((stdOutLine = stdOutput.readLine()) != null) {
                        stdOutputContent.append(stdOutLine);
                    }
                } catch (IOException secondException) {
                    throw ExceptionUtils.firstOrSuppressed(secondException, firstException);
                }
            }
            exitCode = wizard.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    createSpellErrorMessage(exitCode, stdOutputContent, artefacts), e);
        } catch (IOException e) {
            throw new IOException(
                    createSpellErrorMessage(exitCode, stdOutputContent, artefacts), e);
        }

        if (exitCode != 0) {
            throw new IOException(createSpellErrorMessage(exitCode, stdOutputContent, artefacts));
        }
    }

    private String createSpellErrorMessage(
            int exitCode, StringBuilder stdOutputContent, String... artefacts) {
        return new StringBuilder()
                .append("Failed to cast s5cmd spell [")
                .append(String.join(" ", artefacts))
                .append("]")
                .append(String.format(" [exit code = %d]", exitCode))
                .append(" [cfg: ")
                .append(s5CmdConfiguration)
                .append("]")
                .append(" maybe due to:\n")
                .append(stdOutputContent)
                .toString();
    }

    @Nullable
    @Override
    public String getEntropyInjectionKey() {
        return entropyInjectionKey;
    }

    @Override
    public String generateEntropy() {
        return StringUtils.generateRandomAlphanumericString(
                ThreadLocalRandom.current(), entropyLength);
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }

    public String getLocalTmpDir() {
        return localTmpDir;
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        if (s3AccessHelper == null) {
            // this is the case for Presto
            throw new UnsupportedOperationException(
                    "This s3 file system implementation does not support recoverable writers.");
        }

        return S3RecoverableWriter.writer(
                getHadoopFileSystem(),
                tmpFileCreator,
                s3AccessHelper,
                uploadThreadPool,
                s3uploadPartSize,
                maxConcurrentUploadsPerStream);
    }
}
