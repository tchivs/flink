/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.statebackend;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;

import org.rocksdb.AccessHint;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksObject;
import org.rocksdb.WALRecoveryMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Collections.unmodifiableMap;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link ConfigurableRocksDBOptionsFactory} that translates the provided configuration into RocksDB
 * options.
 *
 * <p>{@link DBOptions} and {@link ColumnFamilyOptions} must be prefixed with {@code
 * state.backend.rocksdb.db-options} and {@code state.backend.rocksdb.cf-options} respectively, e.g.
 *
 * <pre>{@code state.backend.rocksdb.db-options.use_direct_reads=false}
 * {@code state.backend.rocksdb.cf-options.bottommost_compression=kDisableCompressionOption}</pre>
 *
 * <p>Some known options are not supported or not allowed to be set (mainly due to compatibility
 * issues).
 *
 * <p>The set of options is fixed, i.e. new RocksDB options would require updating this class to
 * take effect.
 *
 * <p>Implementation note: {@link org.rocksdb.OptionsUtil#loadOptionsFromFile(String, Env,
 * DBOptions, List) loadOptionsFromFile} isn't used to load options transparently because {@link
 * DBOptions} can't be easily merged and don't provide a way to determine whether a particular
 * option was set or a default was used.
 *
 * <p>Unknown options are ignored.
 *
 * <p>Some predefined Flink options might be overridden by these options, e.g. {@code
 * state.backend.rocksdb.log.dir} by {@code db_log_dir}.
 */
public class ConfluentRocksDBOptionsFactory implements ConfigurableRocksDBOptionsFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ConfluentRocksDBOptionsFactory.class);

    private static final String PREFIX = "state.backend.rocksdb.";
    private static final String DB_OPTION_PREFIX = PREFIX + "db-options.";
    private static final String CF_OPTION_PREFIX = PREFIX + "cf-options.";

    private static final String[] UNSUPPORTED_DB_OPTIONS = {
        "skip_log_error_on_recovery", "use_direct_writes"
    };
    private static final String[] UNSUPPORTED_CF_OPTIONS = {
        "compaction_filter_factory",
        "compaction_measure_io_stats",
        "compression_per_level",
        "expanded_compaction_factor",
        "filter_deletes",
        "max_grandparent_overlap_factor",
        "maxbytes_for_level_multiplier_additional",
        "maxwrite_buffer_size_to_maintain",
        "memtable_factory",
        "memtable_prefix_bloom_bits",
        "memtable_prefix_bloom_probes",
        "memtableprefixbloom_huge_page_tlb_size",
        "min_partial_merge_operands",
        "purge_redundant_kvs_while_flush",
        "source_compaction_factor",
        "table_factory",
        "verify_checksums_in_compaction",
    };
    private static final String[] NOT_ALLOWED_CF_OPTIONS = {
        "compaction_filter", "comparator", "merge_operator", "prefix_extractor",
    };
    private static final String NOT_SUPPORTED_MESSAGE = "not supported";
    private static final String NOT_ALLOWED_MESSAGE = "not allowed";

    private static final Map<String, BiConsumer<DBOptions, String>> DB_OPTION_SETTERS =
            unmodifiableMap(getDbOptionSetters());
    private static final Map<String, BiConsumer<ColumnFamilyOptions, String>> CF_OPTION_SETTERS =
            unmodifiableMap(getCfOptionSetters());
    private static final Map<String, CompressionType> COMPRESSION_TYPES =
            unmodifiableMap(getCompressionTypes());

    private transient Map<String, String> dbOptions;
    private transient Map<String, String> cfOptions;

    @Override
    public RocksDBOptionsFactory configure(ReadableConfig configuration) {
        checkState(dbOptions == null && cfOptions == null, "Factory was already configured");
        Map<String, String> dbOptions = new HashMap<>();
        Map<String, String> cfOptions = new HashMap<>();
        for (Map.Entry<String, String> e : ((Configuration) configuration).toMap().entrySet()) {
            String key = e.getKey();
            if (key.startsWith(DB_OPTION_PREFIX)) {
                dbOptions.put(strip(key, DB_OPTION_PREFIX), e.getValue());
            } else if (key.startsWith(CF_OPTION_PREFIX)) {
                cfOptions.put(strip(key, CF_OPTION_PREFIX), e.getValue());
            }
        }

        ensureOptionsNotSet(dbOptions, NOT_SUPPORTED_MESSAGE, UNSUPPORTED_DB_OPTIONS);
        ensureOptionsNotSet(cfOptions, NOT_SUPPORTED_MESSAGE, UNSUPPORTED_CF_OPTIONS);
        ensureOptionsNotSet(cfOptions, NOT_ALLOWED_MESSAGE, NOT_ALLOWED_CF_OPTIONS);

        this.dbOptions = dbOptions;
        this.cfOptions = cfOptions;

        LOG.info("configured with DBOptions: {}, CFOptions: {}", dbOptions, cfOptions);

        return this;
    }

    @Override
    public DBOptions createDBOptions(DBOptions opts, Collection<AutoCloseable> handlesToClose) {
        return setOptions(dbOptions, opts, DB_OPTION_SETTERS);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions opts, Collection<AutoCloseable> handlesToClose) {
        return setOptions(cfOptions, opts, CF_OPTION_SETTERS);
    }

    // T is either DBOptions or ColumnFamilyOptions
    private static <T extends RocksObject> T setOptions(
            Map<String, String> srcConfigMap,
            T dstOpts,
            Map<String, BiConsumer<T, String>> setters) {
        checkNotNull(srcConfigMap, "The factory wasn't configured");
        for (Map.Entry<String, BiConsumer<T, String>> e : setters.entrySet()) {
            String option = e.getKey();
            BiConsumer<T, String> setter = e.getValue();
            if (srcConfigMap.containsKey(option)) {
                setter.accept(dstOpts, srcConfigMap.get(option));
            }
        }
        return dstOpts;
    }

    private static Map<String, BiConsumer<ColumnFamilyOptions, String>> getCfOptionSetters() {
        Map<String, BiConsumer<ColumnFamilyOptions, String>> map = new HashMap<>();
        map.put("arena_block_size", (opts, val) -> opts.setArenaBlockSize(asLong(val)));
        map.put("bloom_locality", (opts, val) -> opts.setBloomLocality(asInt(val)));
        map.put(
                "bottommost_compression",
                (opts, val) -> opts.setBottommostCompressionType(getCompressionType(val)));
        map.put(
                "compaction_pri",
                (opts, val) -> opts.setCompactionPriority(CompactionPriority.valueOf(val)));
        map.put(
                "compaction_style",
                (opts, val) -> opts.setCompactionStyle(CompactionStyle.valueOf(val)));
        map.put("compression", (opts, val) -> opts.setCompressionType(getCompressionType(val)));
        map.put(
                "disable_auto_compactions",
                (opts, val) -> opts.setDisableAutoCompactions(asBoolean(val)));
        map.put(
                "force_consistency_checks",
                (opts, val) -> opts.setForceConsistencyChecks(asBoolean(val)));
        map.put(
                "hard_pending_compaction_bytes_limit",
                (opts, val) -> opts.setHardPendingCompactionBytesLimit(asLong(val)));
        map.put(
                "inplace_update_num_locks",
                (opts, val) -> opts.setInplaceUpdateNumLocks(asLong(val)));
        map.put(
                "inplace_update_support",
                (opts, val) -> opts.setInplaceUpdateSupport(asBoolean(val)));
        map.put(
                "level0_file_num_compaction_trigger",
                (opts, val) -> opts.setLevel0FileNumCompactionTrigger(asInt(val)));
        map.put(
                "level0_slowdown_writes_trigger",
                (opts, val) -> opts.setLevel0SlowdownWritesTrigger(asInt(val)));
        map.put(
                "level0_stop_writes_trigger",
                (opts, val) -> opts.setLevel0StopWritesTrigger(asInt(val)));
        map.put(
                "level_compaction_dynamic_level_bytes",
                (opts, val) -> opts.setLevelCompactionDynamicLevelBytes(asBoolean(val)));
        map.put(
                "max_bytes_for_level_base",
                (opts, val) -> opts.setMaxBytesForLevelBase(asLong(val)));
        map.put(
                "max_bytes_for_level_multiplier",
                (opts, val) -> opts.setMaxBytesForLevelMultiplier(asDouble(val)));
        map.put("max_compaction_bytes", (opts, val) -> opts.setMaxCompactionBytes(asLong(val)));
        map.put(
                "max_sequential_skip_in_iterations",
                (opts, val) -> opts.setMaxSequentialSkipInIterations(asLong(val)));
        map.put("max_successive_merges", (opts, val) -> opts.setMaxSuccessiveMerges(asLong(val)));
        map.put("max_write_buffer_number", (opts, val) -> opts.setMaxWriteBufferNumber(asInt(val)));
        map.put(
                "max_write_buffer_number_to_maintain",
                (opts, val) -> opts.setMaxWriteBufferNumberToMaintain(asInt(val)));
        map.put(
                "max_write_buffer_size_to_maintain",
                (opts, val) -> opts.setMaxWriteBufferNumberToMaintain(asInt(val)));
        map.put(
                "memtable_huge_page_size",
                (opts, val) -> opts.setMemtableHugePageSize(asLong(val)));
        map.put(
                "memtable_prefix_bloom_size_ratio",
                (opts, val) -> opts.setMemtablePrefixBloomSizeRatio(asDouble(val)));
        map.put(
                "min_write_buffer_number_to_merge",
                (opts, val) -> opts.setMinWriteBufferNumberToMerge(asInt(val)));
        map.put(
                "minwrite_buffer_number_to_merge",
                (opts, val) -> opts.setMinWriteBufferNumberToMerge(asInt(val)));
        map.put("num_levels", (opts, val) -> opts.setNumLevels(asInt(val)));
        map.put(
                "optimize_filters_for_hits",
                (opts, val) -> opts.setOptimizeFiltersForHits(asBoolean(val)));
        map.put("paranoid_file_checks", (opts, val) -> opts.setParanoidFileChecks(asBoolean(val)));
        map.put(
                "periodic_compaction_seconds",
                (opts, val) -> opts.setPeriodicCompactionSeconds(asLong(val)));
        map.put("report_bg_io_stats", (opts, val) -> opts.setReportBgIoStats(asBoolean(val)));
        map.put(
                "soft_pending_compaction_bytes_limit",
                (opts, val) -> opts.setSoftPendingCompactionBytesLimit(asLong(val)));
        map.put("target_file_size_base", (opts, val) -> opts.setTargetFileSizeBase(asLong(val)));
        map.put(
                "target_file_size_multiplier",
                (opts, val) -> opts.setTargetFileSizeMultiplier(asInt(val)));
        map.put("ttl", (opts, val) -> opts.setTtl(asLong(val)));
        map.put("write_buffer_size", (opts, val) -> opts.setWriteBufferSize(asLong(val)));
        return map;
    }

    private static Map<String, BiConsumer<DBOptions, String>> getDbOptionSetters() {
        Map<String, BiConsumer<DBOptions, String>> map = new HashMap<>();
        map.put(
                "access_hint_on_compaction_start",
                (opts, val) -> opts.setAccessHintOnCompactionStart(AccessHint.valueOf(val)));
        map.put("advise_random_on_open", (opts, val) -> opts.setAdviseRandomOnOpen(asBoolean(val)));
        map.put("allow_2pc", (opts, val) -> opts.setAllow2pc(Boolean.parseBoolean(val)));
        map.put(
                "allow_concurrent_memtable_write",
                (opts, val) -> opts.setAllowConcurrentMemtableWrite(asBoolean(val)));
        map.put("allow_fallocate", (opts, val) -> opts.setAllowFAllocate(asBoolean(val)));
        map.put("allow_ingest_behind", (opts, val) -> opts.setAllowIngestBehind(asBoolean(val)));
        map.put("allow_mmap_reads", (opts, val) -> opts.setAllowMmapReads(asBoolean(val)));
        map.put("allow_mmap_writes", (opts, val) -> opts.setAllowMmapWrites(asBoolean(val)));
        map.put("atomic_flush", (opts, val) -> opts.setAtomicFlush(asBoolean(val)));
        map.put(
                "avoid_flush_during_recovery",
                (opts, val) -> opts.setAvoidFlushDuringRecovery(asBoolean(val)));
        map.put(
                "avoid_flush_during_shutdown",
                (opts, val) -> opts.setAvoidFlushDuringShutdown(asBoolean(val)));
        map.put(
                "avoid_unnecessary_blocking_io",
                (opts, val) -> opts.setAvoidUnnecessaryBlockingIO(asBoolean(val)));
        map.put(
                "best_efforts_recovery",
                (opts, val) -> opts.setBestEffortsRecovery(asBoolean(val)));
        map.put(
                "bgerror_resume_retry_interval",
                (opts, val) -> opts.setBgerrorResumeRetryInterval(asLong(val)));
        map.put("bytes_per_sync", (opts, val) -> opts.setBytesPerSync(asLong(val)));
        map.put(
                "compaction_readahead_size",
                (opts, val) -> opts.setCompactionReadaheadSize(asLong(val)));
        map.put("create_if_missing", (opts, val) -> opts.setCreateIfMissing(asBoolean(val)));
        map.put(
                "create_missing_column_families",
                (opts, val) -> opts.setCreateMissingColumnFamilies(asBoolean(val)));
        map.put("db_log_dir", (opts, val) -> opts.setDbLogDir(val));
        map.put("db_write_buffer_size", (opts, val) -> opts.setDbWriteBufferSize(asLong(val)));
        map.put("delayed_write_rate", (opts, val) -> opts.setDelayedWriteRate(asLong(val)));
        map.put(
                "delete_obsolete_files_period_micros",
                (opts, val) -> opts.setDeleteObsoleteFilesPeriodMicros(asLong(val)));
        map.put("dump_malloc_stats", (opts, val) -> opts.setDumpMallocStats(asBoolean(val)));
        map.put(
                "enable_pipelined_write",
                (opts, val) -> opts.setEnablePipelinedWrite(asBoolean(val)));
        map.put(
                "enable_thread_tracking",
                (opts, val) -> opts.setEnableThreadTracking(asBoolean(val)));
        map.put(
                "enable_write_thread_adaptive_yield",
                (opts, val) -> opts.setEnableWriteThreadAdaptiveYield(asBoolean(val)));
        map.put("error_if_exists", (opts, val) -> opts.setErrorIfExists(asBoolean(val)));
        map.put(
                "fail_if_options_file_error",
                (opts, val) -> opts.setFailIfOptionsFileError(asBoolean(val)));
        map.put("info_log_level", (opts, val) -> opts.setInfoLogLevel(InfoLogLevel.valueOf(val)));
        map.put("is_fd_close_on_exec", (opts, val) -> opts.setIsFdCloseOnExec(asBoolean(val)));
        map.put("keep_log_file_num", (opts, val) -> opts.setKeepLogFileNum(asLong(val)));
        map.put("log_file_time_to_roll", (opts, val) -> opts.setLogFileTimeToRoll(asLong(val)));
        map.put("log_readahead_size", (opts, val) -> opts.setLogReadaheadSize(asLong(val)));
        map.put(
                "manifest_preallocation_size",
                (opts, val) -> opts.setManifestPreallocationSize(asLong(val)));
        map.put("manual_wal_flush", (opts, val) -> opts.setManualWalFlush(asBoolean(val)));
        map.put(
                "max_background_compactions",
                (opts, val) -> opts.setMaxBackgroundCompactions(asInt(val)));
        map.put("max_background_flushes", (opts, val) -> opts.setMaxBackgroundFlushes(asInt(val)));
        map.put("max_background_jobs", (opts, val) -> opts.setMaxBackgroundJobs(asInt(val)));
        map.put(
                "max_bgerror_resume_count",
                (opts, val) -> opts.setMaxBgErrorResumeCount(asInt(val)));
        map.put(
                "max_file_opening_threads",
                (opts, val) -> opts.setMaxFileOpeningThreads(asInt(val)));
        map.put("max_log_file_size", (opts, val) -> opts.setMaxLogFileSize(asLong(val)));
        map.put("max_manifest_file_size", (opts, val) -> opts.setMaxManifestFileSize(asLong(val)));
        map.put("max_open_files", (opts, val) -> opts.setMaxOpenFiles(asInt(val)));
        map.put("max_subcompactions", (opts, val) -> opts.setMaxSubcompactions(asInt(val)));
        map.put("max_total_wal_size", (opts, val) -> opts.setMaxTotalWalSize(asLong(val)));
        map.put(
                "max_write_batch_group_size_bytes",
                (opts, val) -> opts.setMaxWriteBatchGroupSizeBytes(asLong(val)));
        map.put("paranoid_checks", (opts, val) -> opts.setParanoidChecks(asBoolean(val)));
        map.put("persist_stats_to_disk", (opts, val) -> opts.setPersistStatsToDisk(asBoolean(val)));
        map.put(
                "random_access_max_buffer_size",
                (opts, val) -> opts.setRandomAccessMaxBufferSize(asLong(val)));
        map.put("recycle_log_file_num", (opts, val) -> opts.setRecycleLogFileNum(asLong(val)));
        map.put(
                "skip_checking_sst_file_sizes_on_db_open",
                (opts, val) -> opts.setSkipCheckingSstFileSizesOnDbOpen(asBoolean(val)));
        map.put(
                "skip_stats_update_on_db_open",
                (opts, val) -> opts.setSkipStatsUpdateOnDbOpen(asBoolean(val)));
        map.put("stats_dump_period_sec", (opts, val) -> opts.setStatsDumpPeriodSec(asInt(val)));
        map.put(
                "stats_history_buffer_size",
                (opts, val) -> opts.setStatsHistoryBufferSize(asLong(val)));
        map.put(
                "stats_persist_period_sec",
                (opts, val) -> opts.setStatsPersistPeriodSec(asInt(val)));
        map.put("strict_bytes_per_sync", (opts, val) -> opts.setStrictBytesPerSync(asBoolean(val)));
        map.put(
                "table_cache_numshardbits",
                (opts, val) -> opts.setTableCacheNumshardbits(asInt(val)));
        map.put("two_write_queues", (opts, val) -> opts.setTwoWriteQueues(asBoolean(val)));
        map.put("unordered_write", (opts, val) -> opts.setUnorderedWrite(asBoolean(val)));
        map.put("use_adaptive_mutex", (opts, val) -> opts.setUseAdaptiveMutex(asBoolean(val)));
        map.put(
                "use_direct_io_for_flush_and_compaction",
                (opts, val) -> opts.setUseDirectIoForFlushAndCompaction(asBoolean(val)));
        map.put("use_direct_reads", (opts, val) -> opts.setUseDirectReads(asBoolean(val)));
        map.put("use_fsync", (opts, val) -> opts.setUseFsync(asBoolean(val)));
        map.put("wal_bytes_per_sync", (opts, val) -> opts.setWalBytesPerSync(asLong(val)));
        map.put("wal_dir", (opts, val) -> opts.setWalDir(val));
        map.put(
                "wal_recovery_mode",
                (opts, val) -> opts.setWalRecoveryMode(WALRecoveryMode.valueOf(val)));
        map.put("WAL_size_limit_MB", (opts, val) -> opts.setWalSizeLimitMB(asLong(val)));
        map.put("WAL_ttl_seconds", (opts, val) -> opts.setWalTtlSeconds(asLong(val)));
        map.put(
                "writable_file_max_buffer_size",
                (opts, val) -> opts.setWritableFileMaxBufferSize(asLong(val)));
        map.put(
                "write_dbid_to_manifest",
                (opts, val) -> opts.setWriteDbidToManifest(asBoolean(val)));
        map.put(
                "write_thread_max_yield_usec",
                (opts, val) -> opts.setWriteThreadMaxYieldUsec(asLong(val)));
        map.put(
                "write_thread_slow_yield_usec",
                (opts, val) -> opts.setWriteThreadSlowYieldUsec(asLong(val)));
        return map;
    }

    private static CompressionType getCompressionType(String compressionName) {
        if (COMPRESSION_TYPES.containsKey(compressionName)) {
            return COMPRESSION_TYPES.get(compressionName);
        } else {
            return CompressionType.valueOf(compressionName);
        }
    }

    private static Map<String, CompressionType> getCompressionTypes() {
        // see
        // https://github.com/facebook/rocksdb/blob/9cc0986ae2e652c8d121fcebdd027ac281849e2a/include/rocksdb/compression_type.h#L17
        Map<String, CompressionType> map = new HashMap<>();
        map.put("kNoCompression", CompressionType.NO_COMPRESSION);
        map.put("kSnappyCompression", CompressionType.SNAPPY_COMPRESSION);
        map.put("kZlibCompression", CompressionType.ZLIB_COMPRESSION);
        map.put("kBZlib2Compression", CompressionType.BZLIB2_COMPRESSION);
        map.put("kLZ4Compression", CompressionType.LZ4_COMPRESSION);
        map.put("kLZ4HCCompression", CompressionType.LZ4HC_COMPRESSION);
        map.put("kXpressCompression", CompressionType.XPRESS_COMPRESSION);
        map.put("kZSTD", CompressionType.ZSTD_COMPRESSION);
        map.put("kDisableCompressionOption", CompressionType.DISABLE_COMPRESSION_OPTION);
        return map;
    }

    private void ensureOptionsNotSet(
            Map<String, String> map, String reason, String... disallowedOptions) {
        for (String option : disallowedOptions) {
            if (map.containsKey(option)) {
                throw new UnsupportedOperationException(
                        String.format("Changing %s is %s", option, reason));
            }
        }
    }

    private static long asLong(String v) {
        return Long.parseLong(v);
    }

    private static int asInt(String v) {
        return Integer.parseInt(v);
    }

    private static double asDouble(String v) {
        return Double.parseDouble(v);
    }

    private static boolean asBoolean(String v) {
        return Boolean.parseBoolean(v);
    }

    private String strip(String key, String prefix) {
        return key.substring(prefix.length());
    }
}
