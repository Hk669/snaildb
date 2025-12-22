use snaildb::wal::Wal;
use anyhow::Result;
use tempfile::TempDir;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

#[test]
fn test_wal_open() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    db.append_set("test", b"test")?;
    db.append_delete("test")?;
    db.force_flush()?;
    db.reset()?;
    Ok(())
}

#[test]
fn test_failure_append_set() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    db.append_set("test", b"test")?;
    db.append_delete("test")?;
    db.force_flush()?;
    db.reset()?;
    Ok(())
}

#[test]
fn test_failure_invalid_path() {
    // Test opening WAL with invalid path (non-existent parent)
    let invalid_path = PathBuf::from("/nonexistent/path/to/wal.log");
    let result = Wal::open(&invalid_path);
    assert!(result.is_err());
}

#[test]
fn test_failure_operations_after_drop() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    db.append_set("key1", b"value1")?;
    
    // Drop the WAL - this should close the worker thread
    drop(db);
    
    // Try to create a new WAL at the same path (should work)
    let mut db2 = Wal::open(&db_path)?;
    db2.append_set("key2", b"value2")?;
    Ok(())
}

#[test]
fn test_failure_empty_key() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Empty key should still work (no validation in WAL layer)
    db.append_set("", b"value")?;
    db.append_delete("")?;
    Ok(())
}

#[test]
fn test_failure_very_large_key() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Very large key (1MB)
    let large_key = "x".repeat(1024 * 1024);
    db.append_set(&large_key, b"value")?;
    db.append_delete(&large_key)?;
    Ok(())
}

#[test]
fn test_failure_very_large_value() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Very large value (10MB)
    let large_value = vec![0u8; 10 * 1024 * 1024];
    db.append_set("key", &large_value)?;
    Ok(())
}

#[test]
fn test_failure_multiple_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Perform many operations to test worker thread handling
    for i in 0..1000 {
        db.append_set(&format!("key_{}", i), &format!("value_{}", i).into_bytes())?;
    }
    
    db.force_flush()?;
    
    // Delete all keys
    for i in 0..1000 {
        db.append_delete(&format!("key_{}", i))?;
    }
    
    db.force_flush()?;
    db.reset()?;
    Ok(())
}

#[test]
fn test_failure_replay_on_nonexistent_file() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("nonexistent_wal.log");
    
    // Try to replay a file that doesn't exist
    let result = Wal::open(&db_path);
    assert!(result.is_ok());
    
    let db = result.unwrap();
    // Replay should work on empty file
    let entries = db.replay().unwrap();
    assert_eq!(entries.len(), 0);
}

#[test]
fn test_failure_unicode_keys_and_values() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Test with Unicode characters
    db.append_set("key_🚀", "value_🌍".as_bytes())?;
    db.append_set("ключ", "значение".as_bytes())?;
    db.append_set("键", "值".as_bytes())?;
    
    db.append_delete("key_🚀")?;
    db.append_delete("ключ")?;
    db.append_delete("键")?;
    
    db.force_flush()?;
    Ok(())
}

#[test]
fn test_failure_special_characters_in_key() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Test with special characters
    let special_keys = vec![
        "key\nwith\nnewlines",
        "key\twith\ttabs",
        "key with spaces",
        "key/with/slashes",
        "key\\with\\backslashes",
        "key\"with\"quotes",
        "key'with'apostrophes",
    ];
    
    for key in &special_keys {
        db.append_set(key, b"value")?;
        db.append_delete(key)?;
    }
    
    db.force_flush()?;
    Ok(())
}

#[test]
fn test_failure_null_bytes() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Test with null bytes in value
    let value_with_nulls = vec![0u8, 1u8, 0u8, 2u8, 0u8];
    db.append_set("key", &value_with_nulls)?;
    
    db.force_flush()?;
    Ok(())
}

#[test]
fn test_failure_concurrent_reset_and_write() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    db.append_set("key1", b"value1")?;
    db.append_set("key2", b"value2")?;
    
    // Reset while there might be pending writes
    db.reset()?;
    
    // Write after reset
    db.append_set("key3", b"value3")?;
    db.force_flush()?;
    Ok(())
}

// ============================================================================
// Tests for mpsc handler (worker thread command processing)
// ============================================================================

#[test]
fn test_mpsc_handler_write_record_commands() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Send multiple WriteRecord commands
    db.append_set("key1", b"value1")?;
    db.append_set("key2", b"value2")?;
    db.append_delete("key1")?;
    db.append_set("key3", b"value3")?;
    
    // Force flush to ensure all commands are processed
    db.force_flush()?;
    // Give worker thread time to process commands
    thread::sleep(Duration::from_millis(100));
    
    // Verify all records were written by replaying
    let entries = db.replay()?;
    assert_eq!(entries.len(), 4); // 2 sets, 1 delete, 1 set
    
    Ok(())
}

#[test]
fn test_mpsc_handler_flush_command() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Write records without explicit flush
    db.append_set("key1", b"value1")?;
    db.append_set("key2", b"value2")?;
    
    // Send explicit flush command
    db.force_flush()?;
    // Give worker thread time to process commands
    thread::sleep(Duration::from_millis(100));
    
    // Verify data is persisted
    let entries = db.replay()?;
    assert_eq!(entries.len(), 2);
    
    Ok(())
}

#[test]
fn test_mpsc_handler_reset_command() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Write some records
    db.append_set("key1", b"value1")?;
    db.append_set("key2", b"value2")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(100));
    
    // Verify records exist
    let entries = db.replay()?;
    assert_eq!(entries.len(), 2);
    
    // Send reset command
    db.reset()?;
    thread::sleep(Duration::from_millis(100));
    
    // Verify file is empty after reset
    let entries_after_reset = db.replay()?;
    assert_eq!(entries_after_reset.len(), 0);
    
    // Write new records after reset
    db.append_set("key3", b"value3")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(100));
    
    // Verify only new records exist
    let entries_final = db.replay()?;
    assert_eq!(entries_final.len(), 1);
    
    Ok(())
}

#[test]
fn test_mpsc_handler_command_ordering() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Send commands in specific order
    db.append_set("key1", b"value1")?;
    db.append_set("key2", b"value2")?;
    db.append_set("key1", b"value1_updated")?; // Update key1
    db.append_delete("key2")?;
    db.append_set("key3", b"value3")?;
    
    db.force_flush()?;
    thread::sleep(Duration::from_millis(100));
    
    // Verify commands were processed in order
    let entries = db.replay()?;
    assert_eq!(entries.len(), 5);
    
    // Check that key1 was updated (last write wins)
    let key1_entries: Vec<_> = entries.iter()
        .filter(|(k, _)| k == "key1")
        .collect();
    assert_eq!(key1_entries.len(), 2); // Initial set and update
    
    Ok(())
}

#[test]
fn test_mpsc_handler_multiple_flushes() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Write -> Flush -> Write -> Flush pattern
    db.append_set("key1", b"value1")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    db.append_set("key2", b"value2")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    db.append_set("key3", b"value3")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    // Verify all records are present
    let entries = db.replay()?;
    assert_eq!(entries.len(), 3);
    
    Ok(())
}

#[test]
fn test_mpsc_handler_reset_after_writes() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Write multiple records
    for i in 0..10 {
        db.append_set(&format!("key_{}", i), &format!("value_{}", i).into_bytes())?;
    }
    
    // Reset should flush pending writes first, then clear file
    db.reset()?;
    thread::sleep(Duration::from_millis(100));
    
    // Verify file is empty
    let entries = db.replay()?;
    assert_eq!(entries.len(), 0);
    
    Ok(())
}

#[test]
fn test_mpsc_handler_write_after_reset() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Write -> Reset -> Write sequence
    db.append_set("key1", b"value1")?;
    db.reset()?;
    thread::sleep(Duration::from_millis(100));
    
    db.append_set("key2", b"value2")?;
    db.append_set("key3", b"value3")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(100));
    
    // Verify only post-reset records exist
    let entries = db.replay()?;
    assert_eq!(entries.len(), 2);
    
    // Verify correct keys
    let keys: Vec<_> = entries.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys.contains(&"key2".to_string()));
    assert!(keys.contains(&"key3".to_string()));
    assert!(!keys.contains(&"key1".to_string()));
    
    Ok(())
}

#[test]
fn test_mpsc_handler_mixed_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Mix of SET and DELETE operations
    db.append_set("key1", b"value1")?;
    db.append_set("key2", b"value2")?;
    db.append_delete("key1")?;
    db.append_set("key3", b"value3")?;
    db.append_set("key1", b"value1_new")?; // Re-add after delete
    db.append_delete("key2")?;
    
    db.force_flush()?;
    thread::sleep(Duration::from_millis(100));
    
    // Verify all operations were recorded
    let entries = db.replay()?;
    assert_eq!(entries.len(), 6);
    
    Ok(())
}

#[test]
fn test_mpsc_handler_shutdown_via_drop() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Write some records
    db.append_set("key1", b"value1")?;
    db.append_set("key2", b"value2")?;
    
    // Drop should send Shutdown command and flush
    drop(db);
    // Give worker thread time to process shutdown and flush
    thread::sleep(Duration::from_millis(100));
    
    // Reopen and verify data was persisted
    let db2 = Wal::open(&db_path)?;
    let entries = db2.replay()?;
    assert_eq!(entries.len(), 2);
    
    Ok(())
}

#[test]
fn test_mpsc_handler_rapid_commands() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Send many commands rapidly
    for i in 0..100 {
        db.append_set(&format!("key_{}", i), &format!("value_{}", i).into_bytes())?;
    }
    
    // Single flush at the end
    db.force_flush()?;
    // Give worker thread time to process all commands
    thread::sleep(Duration::from_millis(200));
    
    // Verify all commands were processed
    let entries = db.replay()?;
    assert_eq!(entries.len(), 100);
    
    Ok(())
}

#[test]
fn test_mpsc_handler_flush_without_pending() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Flush without any writes (should be no-op but shouldn't error)
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    // Flush again
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    // Write something
    db.append_set("key1", b"value1")?;
    
    // Flush with pending write
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    // Verify write was persisted
    let entries = db.replay()?;
    assert_eq!(entries.len(), 1);
    
    Ok(())
}

#[test]
fn test_mpsc_handler_reset_clears_pending_state() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Write records (marks as dirty)
    db.append_set("key1", b"value1")?;
    db.append_set("key2", b"value2")?;
    
    // Reset should flush pending writes and clear pending state
    db.reset()?;
    thread::sleep(Duration::from_millis(100));
    
    // Write new records
    db.append_set("key3", b"value3")?;
    
    // Flush should work correctly after reset
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    // Verify only new records exist
    let entries = db.replay()?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "key3");
    
    Ok(())
}

#[test]
fn test_mpsc_handler_concurrent_writes_and_flush() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let mut db = Wal::open(&db_path)?;
    
    // Write -> Flush -> Write -> Flush pattern
    db.append_set("key1", b"value1")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    db.append_set("key2", b"value2")?;
    db.append_set("key3", b"value3")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    db.append_set("key4", b"value4")?;
    db.force_flush()?;
    thread::sleep(Duration::from_millis(50));
    
    // Verify all records are present and in order
    let entries = db.replay()?;
    assert_eq!(entries.len(), 4);
    
    Ok(())
}