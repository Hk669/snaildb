use std::cell::Cell;
use crossbeam_skiplist::SkipMap;

use crate::utils::value::Value;

#[derive(Debug)]
pub struct MemTable {
    entries: SkipMap<String, Value>,
    size_bytes: Cell<usize>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: SkipMap::new(),
            size_bytes: Cell::new(0),
        }
    }

    pub fn insert(&self, key: String, value: Value) {
        // Calculate size: key length + value size + overhead
        let key_size = key.len();
        let value_size = match &value {
            crate::utils::value::Value::Present(bytes) => bytes.len(),
            crate::utils::value::Value::Deleted => 0, // Tombstone has no value bytes
        };
        // Approximate overhead: 8 bytes for String pointer + 8 bytes for Vec pointer + 24 bytes for Value enum
        let new_entry_size = key_size + value_size + 40;
        
        // Calculate the size delta: if replacing, calculate net change; if new, use full size
        let size_delta = if let Some(old_entry) = self.entries.get(&key) {
            // Updating existing entry: calculate net change (new - old)
            let old_value = old_entry.value();
            let old_value_size = match old_value {
                crate::utils::value::Value::Present(bytes) => bytes.len(),
                crate::utils::value::Value::Deleted => 0,
            };
            let old_entry_size = key_size + old_value_size + 40;
            new_entry_size as i64 - old_entry_size as i64
        } else {
            // New entry: add full size
            new_entry_size as i64
        };
        
        // SkipMap::insert takes &self, so we can use &self here
        self.entries.insert(key, value);
        
        // Apply the size delta in one operation
        let current_size = self.size_bytes.get() as i64;
        self.size_bytes.set((current_size + size_delta).max(0) as usize);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        // SkipMap::get returns an EntryRef, we need to clone the value
        self.entries.get(key).map(|entry| entry.value().clone())
    }

    pub fn drain_sorted(&self) -> Vec<(String, Value)> {
        let mut drained = Vec::with_capacity(self.entries.len());
        // SkipMap maintains sorted order, so we can iterate directly
        // Note: crossbeam-skiplist uses epoch-based reclamation, so we need to collect
        // all entries first before clearing
        for entry in self.entries.iter() {
            drained.push((entry.key().clone(), entry.value().clone()));
        }
        // Clear all entries after collecting
        self.entries.clear();
        self.size_bytes.set(0);
        drained
    }

    /// Returns the approximate size of the memtable in bytes
    pub fn size_bytes(&self) -> usize {
        self.size_bytes.get()
    }
}
