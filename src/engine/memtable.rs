use std::collections::BTreeMap;

use crate::utils::{value::Value};

#[derive(Debug)]
pub struct MemTable {
    entries: BTreeMap<String, Value>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: Value) {
        self.entries.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries.get(key)
    }

    pub fn drain_sorted(&mut self) -> Vec<(String, Value)> {
        let mut drained = Vec::with_capacity(self.entries.len());
        for (key, value) in std::mem::take(&mut self.entries) {
            drained.push((key, value));
        }
        drained
    }
}
