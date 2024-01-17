/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.PathsCopyingFileSystem;
import org.apache.flink.core.fs.PathsCopyingFileSystem.CopyTask;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StateBackend.CustomInitializationMetrics;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.guava31.com.google.common.collect.Streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.apache.flink.runtime.metrics.MetricNames.DOWNLOAD_STATE_DURATION;
import static org.apache.flink.util.Preconditions.checkState;

/** Help class for downloading RocksDB state files. */
public class RocksDBStateDownloader extends RocksDBStateDataTransfer {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateDownloader.class);

    private final CustomInitializationMetrics customInitializationMetrics;

    public RocksDBStateDownloader(
            int restoringThreadNum, CustomInitializationMetrics customInitializationMetrics) {
        super(restoringThreadNum);
        this.customInitializationMetrics = customInitializationMetrics;
    }

    /**
     * Transfer all state data to the target directory, as specified in the download requests.
     *
     * @param downloadRequests the list of downloads.
     * @throws Exception If anything about the download goes wrong.
     */
    public void transferAllStateDataToDirectory(
            Collection<StateHandleDownloadSpec> downloadRequests,
            CloseableRegistry closeableRegistry)
            throws Exception {

        // We use this closer for fine-grained shutdown of all parallel downloading.
        CloseableRegistry internalCloser = new CloseableRegistry();
        // Make sure we also react to external close signals.
        closeableRegistry.registerCloseable(internalCloser);
        try {
            long startTimeMs = SystemClock.getInstance().relativeTimeMillis();
            transferAllStateDataToDirectoryAsync(downloadRequests, internalCloser);
            customInitializationMetrics.addMetric(
                    DOWNLOAD_STATE_DURATION,
                    SystemClock.getInstance().relativeTimeMillis() - startTimeMs);
        } catch (Exception e) {
            downloadRequests.stream()
                    .map(StateHandleDownloadSpec::getDownloadDestination)
                    .map(Path::toFile)
                    .forEach(FileUtils::deleteDirectoryQuietly);
            // Error reporting
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            } else {
                throw new FlinkRuntimeException("Failed to download data for state handles.", e);
            }
        } finally {
            // Unregister and close the internal closer.
            if (closeableRegistry.unregisterCloseable(internalCloser)) {
                IOUtils.closeQuietly(internalCloser);
            }
        }
    }

    /** Asynchronously runs the specified download requests on executorService. */
    private void transferAllStateDataToDirectoryAsync(
            Collection<StateHandleDownloadSpec> handleWithPaths,
            CloseableRegistry closeableRegistry)
            throws Exception {
        FutureUtils.waitForAll(
                        createDownloadRunnables(handleWithPaths, closeableRegistry).stream()
                                .map(
                                        runnable ->
                                                CompletableFuture.runAsync(
                                                        runnable, executorService))
                                .collect(Collectors.toList()))
                .get();
    }

    private Collection<Runnable> createDownloadRunnables(
            Collection<StateHandleDownloadSpec> downloadRequests,
            CloseableRegistry closeableRegistry)
            throws IOException {
        // We need to support recovery from multiple FileSystems. At least one scenario that it can
        // happen is when:
        // 1. A checkpoint/savepoint is created on FileSystem_1
        // 2. Job terminates
        // 3. Configuration is changed use checkpoint directory using FileSystem_2
        // 4. Job is restarted from checkpoint (1.) using claim mode
        // 5. New incremental checkpoint is created, that can refer to files both from FileSystem_1
        // and FileSystem_2.
        Map<FileSystem.FSKey, List<CopyTask>> filesSystemsFilesToDownload = new HashMap<>();
        List<Runnable> runnables = new ArrayList<>();

        for (StateHandleDownloadSpec downloadSpec : downloadRequests) {
            for (HandleAndLocalPath handleAndLocalPath : getAllHandles(downloadSpec)) {
                Path downloadDestination =
                        downloadSpec
                                .getDownloadDestination()
                                .resolve(handleAndLocalPath.getLocalPath());
                if (canCopyPaths(handleAndLocalPath.getHandle())) {
                    org.apache.flink.core.fs.Path remotePath =
                            handleAndLocalPath.getHandle().maybeGetPath().get();
                    FileSystem.FSKey newFSKey = new FileSystem.FSKey(remotePath.toUri());
                    List<CopyTask> filesToDownload =
                            filesSystemsFilesToDownload.computeIfAbsent(
                                    newFSKey, fsKey -> new ArrayList<>());
                    filesToDownload.add(
                            new CopyTask(
                                    remotePath,
                                    new org.apache.flink.core.fs.Path(
                                            downloadDestination.toUri())));
                } else {
                    runnables.add(
                            createDownloadRunnableUsingStreams(
                                    handleAndLocalPath.getHandle(),
                                    downloadDestination,
                                    closeableRegistry));
                }
            }
        }

        for (List<CopyTask> filesToDownload : filesSystemsFilesToDownload.values()) {
            checkState(!filesToDownload.isEmpty());
            FileSystem srcFileSystem = FileSystem.get(filesToDownload.get(0).getSrcPath().toUri());
            checkState(srcFileSystem.canCopyPaths());
            runnables.add(
                    createDownloadRunnableUsingCopyFiles(
                            (PathsCopyingFileSystem) srcFileSystem, filesToDownload));
        }

        return runnables;
    }

    private boolean canCopyPaths(StreamStateHandle handle) throws IOException {
        Optional<org.apache.flink.core.fs.Path> remotePath = handle.maybeGetPath();
        if (!remotePath.isPresent()) {
            return false;
        }
        return FileSystem.canCopyPaths(remotePath.get().toUri());
    }

    private Iterable<? extends HandleAndLocalPath> getAllHandles(
            StateHandleDownloadSpec downloadSpec) {
        return Streams.concat(
                        downloadSpec.getStateHandle().getSharedState().stream(),
                        downloadSpec.getStateHandle().getPrivateState().stream())
                .collect(Collectors.toList());
    }

    private Runnable createDownloadRunnableUsingCopyFiles(
            PathsCopyingFileSystem fileSystem, List<CopyTask> copyTasks) {
        LOG.debug("Using copy paths for {} of file system [{}]", copyTasks, fileSystem);
        return ThrowingRunnable.unchecked(() -> fileSystem.copyFiles(copyTasks));
    }

    private Runnable createDownloadRunnableUsingStreams(
            StreamStateHandle remoteFileHandle,
            Path destination,
            CloseableRegistry closeableRegistry) {
        return ThrowingRunnable.unchecked(
                () -> downloadDataForStateHandle(remoteFileHandle, destination, closeableRegistry));
    }

    /** Copies the file from a single state handle to the given path. */
    private void downloadDataForStateHandle(
            StreamStateHandle remoteFileHandle,
            Path restoreFilePath,
            CloseableRegistry closeableRegistry)
            throws IOException {

        if (closeableRegistry.isClosed()) {
            return;
        }

        try {
            FSDataInputStream inputStream = remoteFileHandle.openInputStream();
            closeableRegistry.registerCloseable(inputStream);

            Files.createDirectories(restoreFilePath.getParent());
            OutputStream outputStream = Files.newOutputStream(restoreFilePath, CREATE_NEW);
            closeableRegistry.registerCloseable(outputStream);

            byte[] buffer = new byte[8 * 1024];
            while (true) {
                int numBytes = inputStream.read(buffer);
                if (numBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, numBytes);
            }
            closeableRegistry.unregisterAndCloseAll(outputStream, inputStream);
        } catch (Exception ex) {
            // Quickly close all open streams. This also stops all concurrent downloads because they
            // are registered with the same registry.
            IOUtils.closeQuietly(closeableRegistry);
            throw new IOException(ex);
        }
    }
}
