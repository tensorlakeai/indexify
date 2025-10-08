use std::sync::LazyLock;

/// The number of threads to start for flushing and compaction (default: 1)
pub(super) static ROCKSDB_THREAD_COUNT: LazyLock<i32> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_THREAD_COUNT")
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(1)
});

/// The maximum number of threads to use for flushing and compaction
/// (default: 2)
pub(super) static ROCKSDB_JOBS_COUNT: LazyLock<i32> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_JOBS_COUNT")
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(2)
});

/// The maximum number of write buffers which can be used (default: 2)
pub(super) static ROCKSDB_MAX_WRITE_BUFFER_NUMBER: LazyLock<i32> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_MAX_WRITE_BUFFER_NUMBER")
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(2)
});

/// The amount of data each write buffer can build up in memory (default: 32
/// MiB)
pub(super) static ROCKSDB_WRITE_BUFFER_SIZE: LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_WRITE_BUFFER_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(32 * 1024 * 1024)
});

/// The write-ahead-log size limit in MiB (default: 0)
pub(super) static ROCKSDB_WAL_SIZE_LIMIT: LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_WAL_SIZE_LIMIT")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
});

/// The target file size for compaction in bytes (default: 64 MiB)
pub(super) static ROCKSDB_TARGET_FILE_SIZE_BASE: LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_TARGET_FILE_SIZE_BASE")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(64 * 1024 * 1024)
});

/// The target file size multiplier for each compaction level (default: 2)
pub(super) static ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER: LazyLock<i32> =
    std::sync::LazyLock::new(|| {
        std::env::var("ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER")
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(2)
    });

/// The number of files needed to trigger level 0 compaction (default: 4)
pub(super) static ROCKSDB_LEVEL_ZERO_FILE_COMPACTION_TRIGGER: LazyLock<i32> =
    std::sync::LazyLock::new(|| {
        std::env::var("TL_ROCKSDB_LEVEL_ZERO_FILE_COMPACTION_TRIGGER")
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(4)
    });

/// The maximum number threads which will perform compactions (default: 4)
pub(super) static ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS: LazyLock<u32> =
    std::sync::LazyLock::new(|| {
        std::env::var("TL_ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(4)
    });

/// Whether to use separate queues for WAL writes and memtable writes
/// (default: true)
pub(super) static ROCKSDB_ENABLE_PIPELINED_WRITES: LazyLock<bool> =
    std::sync::LazyLock::new(|| {
        std::env::var("TL_ROCKSDB_ENABLE_PIPELINED_WRITES")
            .ok()
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(true)
    });

/// The maximum number of information log files to keep (default: 10)
pub(super) static ROCKSDB_KEEP_LOG_FILE_NUM: LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_KEEP_LOG_FILE_NUM")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10)
});

/// The information log level of the RocksDB library (default: "warn")
pub(super) static ROCKSDB_LOG_LEVEL: LazyLock<String> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_LOG_LEVEL")
        .ok()
        .and_then(|s| s.parse::<String>().ok())
        .unwrap_or("warn".to_string())
});

/// Use to specify the database compaction style (default: "level")
pub(super) static ROCKSDB_COMPACTION_STYLE: LazyLock<String> = std::sync::LazyLock::new(|| {
    std::env::var("TL_ROCKSDB_COMPACTION_STYLE")
        .ok()
        .and_then(|s| s.parse::<String>().ok())
        .unwrap_or("level".to_string())
});
