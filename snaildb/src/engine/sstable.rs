use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::utils::{
    record::{RecordKind, read_record, write_record},
    value::Value,
};

#[derive(Clone, Debug)]
pub struct Entry {
    key: String,
    value: Value,
}

#[derive(Debug)]
pub struct SsTable {
    path: PathBuf,
    entries: Vec<Entry>,
}

impl SsTable {
    pub fn create(path: impl AsRef<Path>, entries: Vec<(String, Value)>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = File::create(&path)?;
        let entry_count: u32 = entries
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many entries"))?;
        file.write_all(&entry_count.to_le_bytes())?;

        for (key, value) in &entries {
            match value {
                Value::Present(bytes) => {
                    write_record(&mut file, RecordKind::Set, key, bytes)?;
                }
                Value::Deleted => {
                    write_record(&mut file, RecordKind::Delete, key, &[])?;
                }
            }
        }

        file.flush()?;
        file.sync_all()?;

        let stored_entries = entries
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();

        Ok(Self {
            path,
            entries: stored_entries,
        })
    }

    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        let entry_count = read_entry_count(&mut file)?;
        let mut entries = Vec::with_capacity(entry_count as usize);

        for _ in 0..entry_count {
            let record = read_record(&mut file)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "sstable truncated"))?;

            let value = match record.kind {
                RecordKind::Set => Value::from_bytes(record.value),
                RecordKind::Delete => Value::Deleted,
            };

            entries.push(Entry {
                key: record.key,
                value,
            });
        }

        Ok(Self { path, entries })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
            .map(|idx| self.entries[idx].value.clone())
    }
}

fn read_entry_count<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}
