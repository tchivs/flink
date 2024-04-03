/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.statebackend;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.rocksdb.CompressionType.LZ4HC_COMPRESSION;
import static org.rocksdb.CompressionType.SNAPPY_COMPRESSION;

/** {@link ConfluentRocksDBOptionsFactory} test. */
class ConfluentRocksDBOptionsFactoryTest {

    @Test
    void testSetCommonOptions() {
        checkDbOptions(
                singletonMap("state.backend.rocksdb.db-options.max_open_files", "123"),
                opts -> {},
                opts -> assertEquals(123, opts.maxOpenFiles()));
        checkDbOptions(
                singletonMap("state.backend.rocksdb.db-options.max_background_jobs", "123"),
                opts -> {},
                opts -> assertEquals(123, opts.maxBackgroundJobs()));
        checkCfOptions(
                singletonMap("state.backend.rocksdb.cf-options.compression", "kSnappyCompression"),
                opts -> {},
                opts -> assertEquals(SNAPPY_COMPRESSION, opts.compressionType()));
        checkCfOptions(
                singletonMap(
                        "state.backend.rocksdb.cf-options.bottommost_compression",
                        "kLZ4HCCompression"),
                opts -> {},
                opts -> assertEquals(LZ4HC_COMPRESSION, opts.bottommostCompressionType()));
    }

    @Test
    void testPreservingUnsetOptions() {
        checkDbOptions(
                singletonMap("state.backend.rocksdb.db-options.max_background_jobs", "123"),
                opts -> opts.setBytesPerSync(1234),
                opts -> assertEquals(1234, opts.bytesPerSync()));
        checkCfOptions(
                singletonMap("state.backend.rocksdb.cf-options.max_successive_merges", "123"),
                opts -> opts.setArenaBlockSize(1234),
                opts -> assertEquals(1234, opts.arenaBlockSize()));
    }

    @Test
    void testOverridingDbOptions() {
        checkDbOptions(
                singletonMap("state.backend.rocksdb.db-options.max_background_jobs", "123"),
                opts -> opts.setMaxBackgroundJobs(3),
                opts -> assertEquals(123, opts.maxBackgroundJobs()));
        checkCfOptions(
                singletonMap("state.backend.rocksdb.cf-options.arena_block_size", "123"),
                opts -> opts.setArenaBlockSize(3),
                opts -> assertEquals(123, opts.arenaBlockSize()));
    }

    @Test
    void testIgnoringUnknownOptions() {
        checkDbOptions(
                singletonMap("state.backend.rocksdb.db-options.non-existing-option", "123"),
                opts -> {},
                opts -> {});
        checkCfOptions(
                singletonMap("state.backend.rocksdb.cf-options.non-existing-option", "123"),
                opts -> {},
                opts -> {});
        checkDbOptions(
                singletonMap("execution.checkpointing.interval", "123ms"), opts -> {}, opts -> {});
    }

    @Test
    void testDisallowingCertainOptions() {
        assertThrows(
                Exception.class,
                () ->
                        checkDbOptions(
                                singletonMap(
                                        "state.backend.rocksdb.db-options.use_direct_writes",
                                        "true"),
                                opts -> {},
                                opts -> {}));
        assertThrows(
                Exception.class,
                () ->
                        checkCfOptions(
                                singletonMap(
                                        "state.backend.rocksdb.cf-options.prefix_extractor",
                                        "true"),
                                opts -> {},
                                opts -> {}));
    }

    @Test
    void testUninitialized() {
        assertThrows(Exception.class, () -> checkDbOptions(null, opts -> {}, opts -> {}));
        assertThrows(Exception.class, () -> checkCfOptions(null, opts -> {}, opts -> {}));
    }

    @Test
    void testSerializable() throws IOException {
        try (ObjectOutputStream os = new ObjectOutputStream(new ByteArrayOutputStream())) {
            os.writeObject(
                    createFactory(
                            singletonMap(
                                    "state.backend.rocksdb.db-options.non-existing-option",
                                    "123")));
        }
    }

    private void checkDbOptions(
            @Nullable Map<String, String> configMap,
            Consumer<DBOptions> optionsInit,
            Consumer<DBOptions> optionsCheck) {
        checkOptions(
                configMap,
                optionsInit,
                optionsCheck,
                DBOptions::new,
                (factory, opts) -> factory.createDBOptions(opts, new ArrayList<>(), new JobID()));
    }

    private void checkCfOptions(
            @Nullable Map<String, String> configMap,
            Consumer<ColumnFamilyOptions> optionsInit,
            Consumer<ColumnFamilyOptions> optionsCheck) {
        checkOptions(
                configMap,
                optionsInit,
                optionsCheck,
                ColumnFamilyOptions::new,
                (factory, opts) -> factory.createColumnOptions(opts, new ArrayList<>()));
    }

    private <T> void checkOptions(
            @Nullable Map<String, String> configMap,
            Consumer<T> optionsInit,
            Consumer<T> optionsCheck,
            Supplier<T> optionsConstructor,
            BiFunction<ConfluentRocksDBOptionsFactory, T, T> optionsReconstructor) {
        ConfluentRocksDBOptionsFactory factory = createFactory(configMap);
        T opts = optionsConstructor.get();
        optionsInit.accept(opts);
        opts = optionsReconstructor.apply(factory, opts);
        optionsCheck.accept(opts);
    }

    private static ConfluentRocksDBOptionsFactory createFactory(
            @Nullable Map<String, String> configMap) {
        ConfluentRocksDBOptionsFactory factory = new ConfluentRocksDBOptionsFactory();
        if (configMap != null) {
            Configuration configuration = new Configuration();
            for (Map.Entry<String, String> e : configMap.entrySet()) {
                configuration.setString(e.getKey(), e.getValue());
            }
            factory.configure(configuration);
        }
        return factory;
    }
}
