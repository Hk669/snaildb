use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::utils::{RecordKind, read_record, write_record};
use crate::value::Value;

#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    file: std::fs::File,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        Ok(Self { path, file })
    }

    pub fn append_set(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        self.write_record_internal(RecordKind::Set, key, value)
    }

    pub fn append_delete(&mut self, key: &str) -> io::Result<()> {
        self.write_record_internal(RecordKind::Delete, key, &[])
    }

    pub fn replay(&self) -> io::Result<Vec<(String, Value)>> {
        let mut file = std::fs::File::open(&self.path)?;
        let mut entries = Vec::new();

        loop {
            match read_record(&mut file)? {
                Some(record) => {
                    let value = match record.kind {
                        RecordKind::Set => Value::from_bytes(record.value),
                        RecordKind::Delete => Value::tombstone(),
                    };
                    entries.push((record.key, value));
                }
                None => break,
            }
        }

        Ok(entries)
    }

    pub fn reset(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    fn write_record_internal(
        &mut self,
        kind: RecordKind,
        key: &str,
        value: &[u8],
    ) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        write_record(&mut self.file, kind, key, value)?;
        self.file.flush()?;
        self.file.sync_all()
    }
}
