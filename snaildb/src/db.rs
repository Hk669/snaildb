use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::storage::{MemTable, SsTable};
use crate::wal::Wal;
use crate::utils::Value;
use tracing::info;

/// The default flush threshold is 64 MiB (same as RocksDB).
/// This is a safe default for most containerized environments with 512MB-2GB RAM.
const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

/// SnailDb is a struct that represents the database, with the LSM-tree based storage engine, which includes a memtable, a WAL file, and a vector of SSTables.
#[derive(Debug)]
pub struct SnailDb {
    /// The memtable is a in-memory data structure that stores the data that has been written to the database but not yet flushed to disk.
    pub memtable: MemTable,
    /// The WAL is a file that stores the write-ahead log of the database.
    pub wal: Wal,
    /// The SSTables are the immutable on-disk data structures that store the data that has been flushed from the memtable to disk.
    pub sstables: Vec<SsTable>,
    /// The flush threshold is the size of the memtable that triggers a flush to disk, can be set by the user.
    pub flush_threshold_bytes: usize,
    /// The data directory is the directory that stores the database files.
    pub data_dir: PathBuf,
}

impl SnailDb {
    /// Opens the database at the given path, creating it if it doesn't exist.
    pub fn open(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        let wal_path = base_path.join("wal.log");
        let wal = Wal::open(&wal_path)?;
        let memtable = MemTable::new();

        for (key, value) in wal.replay()? {
            memtable.insert(key, value);
        }

        // Load only metadata (bloom filter, min/max keys) for efficient startup
        let mut sstables = load_existing_sstables(&base_path)?;

        Ok(Self {
            memtable,
            wal,
            sstables: {
                sstables.sort_by(|a, b| b.path().cmp(a.path()));
                sstables
            },
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
            data_dir: base_path,
        })
    }

    /// Sets the flush threshold for the database, can be set by the user.
    pub fn with_flush_threshold(mut self, bytes: usize) -> Self {
        self.flush_threshold_bytes = bytes.max(1); // max is to prevent the flush threshold from being set to 0
        self
    }

    /// Writes a key-value pair into the database.
    pub fn put(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Result<()> {
        let key = key.into(); // into is to convert the key to a string
        let value_bytes = value.into();
        self.wal
            .append_set(&key, &value_bytes)
            .with_context(|| "failed to write to WAL")?;
        self.memtable.insert(key, Value::from_bytes(value_bytes));
        if self.memtable.size_bytes() >= self.flush_threshold_bytes {
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Deletes a key from the database.
    pub fn delete(&mut self, key: impl Into<String>) -> Result<()> {
        let key = key.into();
        self.wal
            .append_delete(&key)
            .with_context(|| "failed to write tombstone to WAL")?;
        self.memtable.insert(key, Value::tombstone());
        if self.memtable.size_bytes() >= self.flush_threshold_bytes {
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Gets a value from the database.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(value.as_option());
        }

        // Check each SSTable: bloom filter -> key range -> load entries and search
        // Entries are loaded lazily only when might_contain_key returns true
        for table in &self.sstables {
            if table.might_contain_key(key) {
                if let Some(value) = table.get(key)
                    .with_context(|| format!("failed to read from sstable {}", table.path().display()))? {
                    return Ok(value.as_option());
                }
            }
        }
        Ok(None)
    }

    /// Flushes the memtable to an SSTable.
    pub fn flush_memtable(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let pending = self.memtable.len();
        let file_name = format!("sst-{}.sst", unix_millis());
        let path = self.data_dir.join(file_name);
        info!(
            entry_count = pending,
            path = %path.display(),
            "flushing memtable to SSTable"
        );
        let entries = self.memtable.drain_sorted();
        let table = SsTable::create(&path, entries).with_context(|| "failed to create SSTable")?;
        self.sstables.insert(0, table);
        self.wal.reset().with_context(|| "failed to reset WAL")?;
        info!(
            entry_count = pending,
            path = %path.display(),
            "memtable flush complete"
        );
        Ok(())
    }
}

/// Loads the existing SSTables from the given directory.
/// Only loads metadata (bloom filter, min/max keys) for efficient startup.
/// Entries are loaded lazily when needed.
fn load_existing_sstables(dir: &Path) -> Result<Vec<SsTable>> {
    let mut tables = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "sst" {
                tables.push(
                    SsTable::load_metadata(&path)
                        .with_context(|| format!("failed to load sstable metadata {}", path.display()))?,
                );
            }
        }
    }
    Ok(tables)
}

/// Returns the current time in milliseconds since the UNIX epoch.
fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
