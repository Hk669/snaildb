use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crate::utils::{
    record::{RecordKind, read_record, write_record},
    value::Value,
};

const FLUSH_INTERVAL_MS: u64 = 500;

enum WriteCommand {
    WriteRecord {
        kind: RecordKind,
        key: String,
        value: Vec<u8>,
    },
    Flush,
    Reset,
    Shutdown,
}

#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    sender: mpsc::Sender<WriteCommand>,
    _writer_thread: thread::JoinHandle<()>,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?; //create parent directories if they don't exist
        }
        
        // Use OpenOptions to open/create without truncating existing files
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        
        // Clone path before moving it into the thread
        let wal_path = path.clone();
        
        let (sender, receiver) = mpsc::channel::<WriteCommand>();
        
        // Spawn a dedicated writer thread that:
        //   - Moves the file handle into the thread (owns it)
        //   - Tracks pending_flush state (bool)
        //   - Uses recv_timeout() with FLUSH_INTERVAL_MS timeout
        //   - In the loop, handle:
        //     * WriteRecord: seek to end, write_record(), set pending_flush = true
        //     * Flush: if pending_flush, call flush() and sync_all(), set pending_flush = false
        //     * Reset: flush if pending, then set_len(0), sync_all(), seek to start
        //     * Shutdown: flush if pending, then break
        //     * Timeout: if pending_flush, flush and sync, set pending_flush = false
        //     * Disconnected: flush if pending, then break
        let write_thread = thread::spawn(move || {
            let mut file = file;
            let mut pending_flush = false;
            let flush_interval = Duration::from_millis(FLUSH_INTERVAL_MS);

            loop {
                match receiver.recv_timeout(flush_interval) {
                    Ok(WriteCommand::WriteRecord { kind, key, value }) => {
                        if let Err(e) = file.seek(SeekFrom::End(0)) {
                            eprintln!("WAL seek error: {}", e);
                            continue;
                        }
                        if let Err(e) = write_record(&mut file, kind, &key, &value) {
                            eprintln!("WAL write error: {}", e);
                            continue;
                        }
                        pending_flush = true;
                    }
                    Ok(WriteCommand::Flush) => {
                        if pending_flush {
                            if let Err(e) = file.flush() {
                                eprintln!("WAL flush error: {}", e);
                            }
                            if let Err(e) = file.sync_all() {
                                eprintln!("WAL sync error: {}", e);
                            }
                            pending_flush = false;
                        }
                    }
                    Ok(WriteCommand::Reset) => {
                        // Flush before reset
                        if pending_flush {
                            if let Err(e) = file.flush() {
                                eprintln!("WAL flush error: {}", e);
                            }
                            if let Err(e) = file.sync_all() {
                                eprintln!("WAL sync error: {}", e);
                            }
                            pending_flush = false;
                        }
                        // Reset the file
                        if let Err(e) = file.set_len(0) {
                            eprintln!("WAL reset error: {}", e);
                        }
                        if let Err(e) = file.sync_all() {
                            eprintln!("WAL sync error: {}", e);
                        }
                        if let Err(e) = file.seek(SeekFrom::Start(0)) {
                            eprintln!("WAL seek error: {}", e);
                        }
                    }
                    Ok(WriteCommand::Shutdown) => {
                        if pending_flush {
                            if let Err(e) = file.flush() {
                                eprintln!("WAL flush error: {}", e);
                            }
                            if let Err(e) = file.sync_all() {
                                eprintln!("WAL sync error: {}", e);
                            }
                        }
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // Periodic flush interval reached
                        if pending_flush {
                            if let Err(e) = file.flush() {
                                eprintln!("WAL flush error: {}", e);
                            }
                            if let Err(e) = file.sync_all() {
                                eprintln!("WAL sync error: {}", e);
                            }
                            pending_flush = false;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // Channel closed, flush and exit
                        if pending_flush {
                            if let Err(e) = file.flush() {
                                eprintln!("WAL flush error: {}", e);
                            }
                            if let Err(e) = file.sync_all() {
                                eprintln!("WAL sync error: {}", e);
                            }
                        }
                        break;
                    }
                }
            }
        });
        
        Ok(Wal {
            path: wal_path,
            sender,
            _writer_thread: write_thread,
        })
    }

    pub fn append_set(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        self.write_record_internal(RecordKind::Set, key, value)
    }
    
    pub fn append_delete(&mut self, key: &str) -> io::Result<()> {
        self.write_record_internal(RecordKind::Delete, key, &[])
    }
    pub fn replay(&self) -> io::Result<Vec<(String, Value)>> {
        // TODO: Open the WAL file for reading (separate handle from writer thread)
        let mut file = File::open(&self.path)?;
        let mut entries = Vec::new();
        while let Some(record) = read_record(&mut file)? {
            match record.kind {
                RecordKind::Set => {
                    entries.push((record.key, Value::from_bytes(record.value)));
                }
                RecordKind::Delete => {
                    entries.push((record.key, Value::tombstone()));
                }
            }
        }
        Ok(entries)
    }

    /// Force an immediate flush and sync of the WAL file.
    /// This is useful for critical operations that require durability guarantees.
    pub fn force_flush(&self) -> io::Result<()> {
        self.sender.send(WriteCommand::Flush).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("WAL force_flush error: {}", e)))?;
        Ok(())
    }

    pub fn reset(&mut self) -> io::Result<()> {
        self.sender.send(WriteCommand::Reset).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("WAL reset error: {}", e)))?;
        Ok(())
    }

    fn write_record_internal(
        &mut self,
        kind: RecordKind,
        key: &str,
        value: &[u8],
    ) -> io::Result<()> {
                // TODO: Send WriteCommand::WriteRecord through the channel
        self.sender
        .send(WriteCommand::WriteRecord { kind, key: key.to_string(), value: value.to_vec() })
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("WAL channel error: {}", e)))?;
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.sender.send(WriteCommand::Shutdown).unwrap();
    }
}
