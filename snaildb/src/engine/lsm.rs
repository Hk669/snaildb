use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::engine::memtable::MemTable;
use crate::engine::sstable::SsTable;
use crate::engine::wal::Wal;
use crate::utils::value::Value;
use tracing::info;

const DEFAULT_FLUSH_THRESHOLD: usize = 128;

#[derive(Debug)]
pub struct LsmTree {
    pub memtable: MemTable,
    pub wal: Wal,
    pub sstables: Vec<SsTable>,
    pub flush_threshold: usize,
    pub data_dir: PathBuf,
}

impl LsmTree {
    pub fn open(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        let wal_path = base_path.join("wal.log");
        let wal = Wal::open(&wal_path)?;
        let memtable = MemTable::new();

        for (key, value) in wal.replay()? {
            memtable.insert(key, value);
        }

        let mut sstables = load_existing_sstables(&base_path)?;

        Ok(Self {
            memtable,
            wal,
            sstables: {
                sstables.sort_by(|a, b| b.path().cmp(a.path()));
                sstables
            },
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            data_dir: base_path,
        })
    }

    pub fn with_flush_threshold(mut self, entries: usize) -> Self {
        self.flush_threshold = entries.max(1); // max is to prevent the flush threshold from being set to 0
        self
    }

    pub fn put(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Result<()> {
        let key = key.into(); // into is to convert the key to a string
        let value_bytes = value.into();
        self.wal
            .append_set(&key, &value_bytes)
            .with_context(|| "failed to write to WAL")?;
        self.memtable.insert(key, Value::from_bytes(value_bytes));
        if self.memtable.len() >= self.flush_threshold {
            self.flush_memtable()?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: impl Into<String>) -> Result<()> {
        let key = key.into();
        self.wal
            .append_delete(&key)
            .with_context(|| "failed to write tombstone to WAL")?;
        self.memtable.insert(key, Value::tombstone());
        if self.memtable.len() >= self.flush_threshold {
            self.flush_memtable()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(value.as_option());
        }

        for table in &self.sstables {
            if let Some(value) = table.get(key) {
                return Ok(value.as_option());
            }
        }

        Ok(None)
    }

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

fn load_existing_sstables(dir: &Path) -> Result<Vec<SsTable>> {
    let mut tables = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "sst" {
                tables.push(
                    SsTable::load(&path)
                        .with_context(|| format!("failed to load sstable {}", path.display()))?,
                );
            }
        }
    }
    Ok(tables)
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
