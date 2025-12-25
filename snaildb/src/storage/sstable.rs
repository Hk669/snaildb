use std::fs::File;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::cell::RefCell;

use crate::storage::bloom_filter::BloomFilter;
use crate::utils::{
    record::{RecordKind, read_record, write_record},
    value::Value,
};

#[derive(Clone, Debug)]
pub struct Entry {
    /// the key of the entry
    key: String,
    /// the value of the entry
    value: Value,
}

#[derive(Clone, Debug)]
pub struct SsTableMetadata {
    /// the path to the sstable file
    path: PathBuf, 
    /// the minimum key in the sstable
    min_key: String, 
    /// the maximum key in the sstable
    max_key: String,
    /// the bloom filter for the sstable
    pub bloom_filter: BloomFilter,
}

#[derive(Debug)]
pub struct SsTable {
    pub metadata: SsTableMetadata,
    /// Entries are loaded lazily - None means not loaded yet, Some means loaded
    entries: RefCell<Option<Vec<Entry>>>,
}

impl SsTable {
    pub fn create(path: impl AsRef<Path>, entries: Vec<(String, Value)>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Calculate min/max keys
        let min_key = entries.first().map(|(key, _)| key.clone()).unwrap();
        let max_key = entries.last().map(|(key, _)| key.clone()).unwrap();

        // Build bloom filter with all keys
        let mut bloom_filter = BloomFilter::new(entries.len());
        for (key, _) in &entries {
            bloom_filter.insert(key);
        }

        let mut file = File::create(&path)?;
        
        // Write header: [entry_count:4][bloom_size:4][bloom_data:var]
        let entry_count: u32 = entries
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many entries"))?;
        file.write_all(&entry_count.to_le_bytes())?;
        
        let bloom_size: u32 = bloom_filter.bits.len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "bloom filter too large"))?;
        file.write_all(&bloom_size.to_le_bytes())?;
        file.write_all(&bloom_filter.bits)?;

        // Write data section: records
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

        // Write footer: [min_key_len:4][min_key:var][max_key_len:4][max_key:var][footer_offset:8]
        let footer_offset = file.stream_position()?;
        file.write_all(&(min_key.len() as u32).to_le_bytes())?;
        file.write_all(min_key.as_bytes())?;
        file.write_all(&(max_key.len() as u32).to_le_bytes())?;
        file.write_all(max_key.as_bytes())?;
        file.write_all(&footer_offset.to_le_bytes())?;  // 8 bytes, always last

        file.flush()?;
        file.sync_all()?;

        let stored_entries = entries
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();

        let metadata = SsTableMetadata {
            path,
            min_key,
            max_key,
            bloom_filter,
        };

        Ok(Self {
            metadata,
            entries: RefCell::new(Some(stored_entries)),
        })
    }

    /// Loads only metadata (bloom filter, min/max keys) without loading entries into memory.
    /// This is efficient for startup when you only need to check if keys might exist.
    pub fn load_metadata(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        
        // Read header: [entry_count:4][bloom_size:4][bloom_data:var]
        let _entry_count = read_entry_count(&mut file)?;
        let bloom_size = read_u32(&mut file, "bloom_size")?;
        let mut bloom_bits = vec![0u8; bloom_size as usize];
        file.read_exact(&mut bloom_bits)?;
        let bloom_filter = BloomFilter { bits: bloom_bits };

        // Read footer (we need to skip the data section)
        let (min_key, max_key) = read_footer(&mut file)?;

        let metadata = SsTableMetadata {
            path,
            min_key,
            max_key,
            bloom_filter,
        };

        Ok(Self {
            metadata,
            entries: RefCell::new(None), // Entries not loaded yet
        })
    }

    /// Loads the full SSTable including all entries into memory.
    /// Use this when you need to access entries directly.
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        
        // Read header: [entry_count:4][bloom_size:4][bloom_data:var]
        let entry_count = read_entry_count(&mut file)?;
        let bloom_size = read_u32(&mut file, "bloom_size")?;
        let mut bloom_bits = vec![0u8; bloom_size as usize];
        file.read_exact(&mut bloom_bits)?;
        let bloom_filter = BloomFilter { bits: bloom_bits };

        // Read data section: records
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

        // Read footer
        let (min_key, max_key) = read_footer(&mut file)?;

        let metadata = SsTableMetadata {
            path,
            min_key,
            max_key,
            bloom_filter,
        };

        Ok(Self {
            metadata,
            entries: RefCell::new(Some(entries)),
        })
    }

    /// Ensures entries are loaded into memory. Loads them from disk if not already loaded.
    fn ensure_entries_loaded(&self) -> io::Result<()> {
        // Check if already loaded
        if self.entries.borrow().is_some() {
            return Ok(());
        }

        // Load entries from disk
        let mut file = File::open(&self.metadata.path)?;
        
        // Read header: [entry_count:4][bloom_size:4][bloom_data:var]
        let entry_count = read_entry_count(&mut file)?;
        let bloom_size = read_u32(&mut file, "bloom_size")?;
        // Skip bloom filter (we already have it in metadata)
        file.seek(SeekFrom::Current(bloom_size as i64))?;

        // Read data section: records
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

        // Store loaded entries
        *self.entries.borrow_mut() = Some(entries);
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.metadata.path
    }

    pub fn get(&self, key: &str) -> io::Result<Option<Value>> {
        // Load entries if not already loaded
        self.ensure_entries_loaded()?;
        
        let entries = self.entries.borrow();
        let entries = entries.as_ref().unwrap(); // Safe because ensure_entries_loaded guarantees Some
        
        Ok(entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
            .map(|idx| entries[idx].value.clone()))
    }

    pub fn might_contain_key(&self, key: &str) -> bool {
        // First check bloom filter for fast negative check
        if !self.metadata.bloom_filter.may_contain(key) {
            return false;
        }
        // Then check key range
        key >= self.metadata.min_key.as_str() && key <= self.metadata.max_key.as_str()
    }
}

fn read_entry_count<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u32<R: Read>(reader: &mut R, label: &str) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(|err| {
        io::Error::new(err.kind(), format!("unable to read {label}: {err}"))
    })?;
    Ok(u32::from_le_bytes(buf))
}

fn read_footer<R: Read + Seek>(reader: &mut R) -> io::Result<(String, String)> {
    // 1. Read footer_offset from the last 8 bytes
    reader.seek(SeekFrom::End(-8))?;
    let mut offset_buf = [0u8; 8];
    reader.read_exact(&mut offset_buf)?;
    let footer_offset = u64::from_le_bytes(offset_buf);

    // 2. Seek to footer start and read min_key
    reader.seek(SeekFrom::Start(footer_offset))?;
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let min_key_len = u32::from_le_bytes(len_buf) as usize;
    let mut min_key_bytes = vec![0u8; min_key_len];
    reader.read_exact(&mut min_key_bytes)?;
    let min_key = String::from_utf8(min_key_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("invalid min_key: {e}")))?;

    // 3. Read max_key
    reader.read_exact(&mut len_buf)?;
    let max_key_len = u32::from_le_bytes(len_buf) as usize;
    let mut max_key_bytes = vec![0u8; max_key_len];
    reader.read_exact(&mut max_key_bytes)?;
    let max_key = String::from_utf8(max_key_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("invalid max_key: {e}")))?;

    Ok((min_key, max_key))
}