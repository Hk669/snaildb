use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::wal::enums::WriteCommand;
use crate::wal::{FLUSH_INTERVAL_MS, SyncManager};
use crate::worker::handler::WorkerManager;

use crate::utils::{RecordKind, read_record, encode_batch_records, Value};

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

/// Writes the batch buffer to file if it's not empty, marks dirty, and clears it.
fn write_batch_if_needed(
    file: &mut File,
    sync_manager: &mut SyncManager,
    batch_buffer: &mut Vec<u8>,
) {
    if !batch_buffer.is_empty() {
        if let Err(e) = file.write_all(batch_buffer) {
            eprintln!("WAL write error: {}", e);
        } else {
            sync_manager.mark_dirty();
        }
        batch_buffer.clear();
    }
}

/// Handles a flush command: writes any pending batch and flushes to disk.
fn handle_flush(
    file: &mut File,
    sync_manager: &mut SyncManager,
    batch_buffer: &mut Vec<u8>,
) {
    write_batch_if_needed(file, sync_manager, batch_buffer);
    if let Err(e) = sync_manager.flush_if_pending_file(file) {
        eprintln!("WAL flush error: {}", e);
    }
}

/// Handles a reset command: writes batch, flushes, truncates file, and clears state.
fn handle_reset(
    file: &mut File,
    sync_manager: &mut SyncManager,
    batch_buffer: &mut Vec<u8>,
) {
    write_batch_if_needed(file, sync_manager, batch_buffer);
    
    // Flush before reset to ensure all data is persisted
    if let Err(e) = sync_manager.flush_if_pending_file(file) {
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
    let mut batch_buffer = Vec::with_capacity(8192);

    loop {
        match receiver.recv_timeout(timeout) {
            Ok(WriteCommand::WriteRecord { kind, key, value }) => {
                // Batch writes to avoid syscall overhead.
                // Clear buffer but keep capacity to avoid reallocations
                batch_buffer.clear();

                // Encode first record into buffer
                if let Err(e) = encode_batch_records(&mut batch_buffer, kind, &key, &value) {
                    eprintln!("WAL encode error: {}", e);
                    continue;
                }
                
                let mut should_write_batch = true;
                let batch_start_time = Instant::now();
                
                // Try to drain more WriteRecord commands (non-blocking)
                loop {
                    // Check if flush interval has elapsed since batch start
                    if batch_start_time.elapsed() >= Duration::from_millis(FLUSH_INTERVAL_MS) {
                        break;
                    }
                    
                    match receiver.try_recv() {
                        Ok(WriteCommand::WriteRecord { kind, key, value }) => {
                            // Encode this record into the batch buffer
                            if let Err(e) = encode_batch_records(&mut batch_buffer, kind, &key, &value) {
                                eprintln!("WAL encode error: {}", e);
                                break; // Write what we have so far
                            }
                        }
                        Ok(WriteCommand::Flush) => {
                            handle_flush(&mut file, &mut sync_manager, &mut batch_buffer);
                            should_write_batch = false; // Already wrote and flushed
                            break;
                        }
                        Ok(WriteCommand::Reset) => {
                            handle_reset(&mut file, &mut sync_manager, &mut batch_buffer);
                            should_write_batch = false; // Already handled reset
                            break;
                        }
                        Ok(WriteCommand::Shutdown) => {
                            write_batch_if_needed(&mut file, &mut sync_manager, &mut batch_buffer);
                            // Force flush on shutdown
                            if let Err(e) = sync_manager.force_flush(&mut file) {
                                eprintln!("WAL flush error: {}", e);
                            }
                            return; // Exit the handler loop
                        }
                        Err(mpsc::TryRecvError::Empty) => {
                            // No more commands available, exit batching loop
                            break;
                        }
                        Err(mpsc::TryRecvError::Disconnected) => {
                            // Channel closed, write batch and exit
                            write_batch_if_needed(&mut file, &mut sync_manager, &mut batch_buffer);
                            if let Err(e) = sync_manager.force_flush(&mut file) {
                                eprintln!("WAL flush error: {}", e);
                            }
                            return; // Exit the handler loop
                        }
                    }
                }
                
                // Write the entire batch in ONE syscall (if not already written)
                if should_write_batch {
                    write_batch_if_needed(&mut file, &mut sync_manager, &mut batch_buffer);
                }
            }
            
            Ok(WriteCommand::Flush) => {
                handle_flush(&mut file, &mut sync_manager, &mut batch_buffer);
            }
            
            Ok(WriteCommand::Reset) => {
                handle_reset(&mut file, &mut sync_manager, &mut batch_buffer);
            }
            
            Ok(WriteCommand::Shutdown) => {
                write_batch_if_needed(&mut file, &mut sync_manager, &mut batch_buffer);
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
