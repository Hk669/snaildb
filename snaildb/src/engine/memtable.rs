use crossbeam_skiplist::SkipMap;

use crate::utils::value::Value;

#[derive(Debug)]
pub struct MemTable {
    entries: SkipMap<String, Value>,
    mutable : bool,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: SkipMap::new(),
            mutable: true,
        }
    }

    pub fn insert(&self, key: String, value: Value) {
        // SkipMap::insert takes &self, so we can take &self here instead of &mut self
        self.entries.insert(key, value);
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
        drained
    }
}
