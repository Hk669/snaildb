use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use crate::wal::enums::WriteCommand;
use crate::wal::db_sync::SyncManager;
use crate::worker::handler::WorkerManager;

use crate::utils::{RecordKind, read_record, write_record, Value};

/// WAL (Write-Ahead Log) provides durable write operations.
/// 
/// Writes are sent to a background thread that handles file I/O,
/// ensuring that write operations don't block the main thread.
#[derive(Debug)]
pub struct Wal {
    /// The path to the WAL file.
    pub path: PathBuf,
    /// The worker manager that handles the background thread for the WAL.
    pub worker: WorkerManager<WriteCommand>,
}

impl Wal {
    /// Opens a WAL file at the given path, creating it if it doesn't exist.
    /// 
    /// This spawns a background worker thread that handles all file I/O operations.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        // Open the file handle - this will be moved into the worker thread
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true) // append mode automatically moves the cursor to end of file, eliminating seek overhead costing write performance everytime we write a record to the file.
            .open(&path)?;
        
        let wal_path = path.clone();
        let flush_interval = Duration::from_millis(crate::wal::db_sync::FLUSH_INTERVAL_MS);
        
        // Spawn the worker thread using WorkerManager
        let worker = WorkerManager::spawn(
            move |receiver, timeout| {
                wal_handler(receiver, timeout, file);
            },
            flush_interval,
        );
        
        Ok(Wal {
            path: wal_path,
            worker,
        })
    }

    /// Appends a SET record to the WAL.
    pub fn append_set(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        self.write_record_internal(RecordKind::Set, key, value)
    }
    
    /// Appends a DELETE record (tombstone) to the WAL.
    pub fn append_delete(&mut self, key: &str) -> io::Result<()> {
        self.write_record_internal(RecordKind::Delete, key, &[])
    }

    /// Replays all records from the WAL file.
    /// 
    /// Opens a separate read handle to avoid conflicts with the writer thread.
    pub fn replay(&self) -> io::Result<Vec<(String, Value)>> {
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

    /// Forces an immediate flush and sync of the WAL file.
    /// 
    /// This is useful for critical operations that require durability guarantees.
    pub fn force_flush(&self) -> io::Result<()> {
        self.worker
            .send(WriteCommand::Flush)
            .map_err(|e| io::Error::new(
                io::ErrorKind::Other,
                format!("WAL force_flush error: {}", e)
            ))?;
        Ok(())
    }

    /// Resets the WAL file (truncates to zero length).
    /// 
    /// This is typically called after flushing the memtable to SSTable.
    pub fn reset(&mut self) -> io::Result<()> {
        self.worker
            .send(WriteCommand::Reset)
            .map_err(|e| io::Error::new(
                io::ErrorKind::Other,
                format!("WAL reset error: {}", e)
            ))?;
        Ok(())
    }

    /// Writes a record to the WAL file, internal function.
    fn write_record_internal(
        &mut self,
        kind: RecordKind,
        key: &str,
        value: &[u8],
    ) -> io::Result<()> {
        self.worker
            .send(WriteCommand::WriteRecord {
                kind,
                key: key.to_string(),
                value: value.to_vec(),
            })
            .map_err(|e| io::Error::new(
                io::ErrorKind::Other,
                format!("WAL channel error: {}", e)
            ))?;
        Ok(())
    }
}

/// The worker thread handler that processes WAL commands.
/// 
/// This function runs in a dedicated background thread and handles:
/// - Writing records to the WAL file
/// - Flushing and syncing for durability
/// - Resetting the file when needed
/// - Periodic automatic flushes
fn wal_handler(
    receiver: mpsc::Receiver<WriteCommand>,
    timeout: Duration,
    mut file: File,
) {
    let mut sync_manager = SyncManager::new();

    loop {
        match receiver.recv_timeout(timeout) {
            Ok(WriteCommand::WriteRecord { kind, key, value }) => {
                // the seek over head is not required, as the append mode points the cursor directly to the end.
                if let Err(e) = write_record(&mut file, kind, &key, &value) {
                    eprintln!("WAL write error: {}", e);
                    continue;
                }
                sync_manager.mark_dirty();
            }
            
            Ok(WriteCommand::Flush) => {
                // Flush if there are pending writes
                if let Err(e) = sync_manager.flush_if_pending_file(&mut file) {
                    eprintln!("WAL flush error: {}", e);
                }
            }
            
            Ok(WriteCommand::Reset) => {
                // Flush before reset to ensure all data is persisted
                if let Err(e) = sync_manager.flush_if_pending_file(&mut file) {
                    eprintln!("WAL flush error: {}", e);
                }
                
                // Reset the file (truncate to zero)
                if let Err(e) = file.set_len(0) {
                    eprintln!("WAL reset error: {}", e);
                }
                if let Err(e) = file.sync_all() {
                    eprintln!("WAL sync error: {}", e);
                }
                if let Err(e) = file.seek(SeekFrom::Start(0)) {
                    eprintln!("WAL seek error: {}", e);
                }
                
                // Clear pending state after reset since file is empty
                sync_manager.clear_pending();
            }
            
            Ok(WriteCommand::Shutdown) => {
                // Force flush on shutdown to ensure all data is persisted
                if let Err(e) = sync_manager.force_flush(&mut file) {
                    eprintln!("WAL flush error: {}", e);
                }
                break;
            }
            
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Periodic flush interval reached - flush if there are pending writes
                if let Err(e) = sync_manager.flush_if_pending_file(&mut file) {
                    eprintln!("WAL flush error: {}", e);
                }
            }
            
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // Channel closed (sender dropped), flush and exit
                if let Err(e) = sync_manager.force_flush(&mut file) {
                    eprintln!("WAL flush error: {}", e);
                }
                break;
            }
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Send shutdown command to ensure clean exit
        // Ignore errors since we're dropping anyway
        let _ = self.worker.send(WriteCommand::Shutdown);
    }
}
