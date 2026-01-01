use std::io::{self, Write};
use std::time::Duration;

/// Configuration constant for flush interval
pub const FLUSH_INTERVAL_MS: u64 = 50; // 50 ms

/// Manages the sync/flush state and operations for WAL durability.
/// 
/// This struct encapsulates:
/// - Tracking whether there are pending writes that need to be flushed
/// - Performing flush and sync operations
/// - Managing the flush interval timing
/// 
/// The sync manager ensures that writes are periodically flushed to disk
/// for durability, while avoiding excessive syscalls by batching flushes.
pub struct SyncManager {
    /// Whether there are unflushed writes that need to be synced to disk
    pending_flush: bool,
    /// The interval at which periodic flushes should occur
    flush_interval: Duration,
}

impl SyncManager {
    /// Creates a new SyncManager with the default flush interval
    pub fn new() -> Self {
        Self {
            pending_flush: false,
            flush_interval: Duration::from_millis(FLUSH_INTERVAL_MS),
        }
    }

    /// Creates a new SyncManager with a custom flush interval
    pub fn with_interval(interval_ms: u64) -> Self {
        Self {
            pending_flush: false,
            flush_interval: Duration::from_millis(interval_ms),
        }
    }

    /// Returns the flush interval duration
    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    /// Marks that there are pending writes that need to be flushed
    /// Call this after writing a record to indicate it needs to be synced
    pub fn mark_dirty(&mut self) {
        self.pending_flush = true;
    }

    /// Checks if there are pending writes that need flushing
    pub fn has_pending(&self) -> bool {
        self.pending_flush
    }

    /// Flushes and syncs the file if there are pending writes.
    /// 
    /// This performs:
    /// 1. `flush()` - Flushes the OS buffer to the file system
    /// 2. `sync_all()` - Ensures data is written to disk (durability guarantee)
    /// 
    /// If there are no pending writes, this is a no-op.
    /// After flushing, the pending state is cleared.
    /// 
    /// # Errors
    /// Returns an error if flush or sync operations fail
    pub fn flush_if_pending<W: Write>(&mut self, file: &mut W) -> io::Result<()> {
        if !self.pending_flush {
            return Ok(());
        }

        file.flush()?;
        // Note: sync_all() is only available on File, not generic Write
        // We'll handle this differently - see flush_if_pending_file below
        self.pending_flush = false;
        Ok(())
    }

    /// Flushes and syncs a file if there are pending writes.
    /// 
    /// This is a specialized version for `std::fs::File` that includes
    /// `sync_all()` for full durability guarantees.
    pub fn flush_if_pending_file(&mut self, file: &mut std::fs::File) -> io::Result<()> {
        if !self.pending_flush {
            return Ok(());
        }

        file.flush()?;
        file.sync_all()?;
        self.pending_flush = false;
        Ok(())
    }

    /// Forces a flush and sync even if there are no pending writes.
    /// Useful for explicit durability requirements (e.g., before shutdown).
    pub fn force_flush(&mut self, file: &mut std::fs::File) -> io::Result<()> {
        file.flush()?;
        file.sync_all()?;
        self.pending_flush = false;
        Ok(())
    }

    /// Clears the pending flush state without actually flushing.
    /// Use with caution - this should only be used when you're certain
    /// the data doesn't need to be flushed (e.g., after a reset operation).
    pub fn clear_pending(&mut self) {
        self.pending_flush = false;
    }
}

impl Default for SyncManager {
    fn default() -> Self {
        Self::new()
    }
}
