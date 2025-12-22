use snaildb::SnailDb;
use anyhow::Result;
use tempfile::TempDir;

#[test]
fn test_basic_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    
    // Open or create a database at a directory
    let mut db = SnailDb::open(&db_path)?;
    
    // Store a key-value pair
    db.put("user:1", b"Alice")?;
    db.put("user:2", b"Bob")?;
    
    // Retrieve a value
    match db.get("user:1")? {
        Some(value) => {
            let value_str = String::from_utf8_lossy(&value);
            assert_eq!(value_str, "Alice");
        }
        None => panic!("Key user:1 should exist"),
    }
    
    // Delete a key
    db.delete("user:2")?;
    
    // Verify deletion
    assert_eq!(db.get("user:2")?, None);
    
    Ok(())
}

#[test]
fn test_custom_flush_threshold() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    
    let mut db = SnailDb::open(&db_path)?
        .with_flush_threshold(256 * 1024 * 1024); // Flush memtable after 256 MiB (for testing)
    
    // Verify it works
    db.put("test", b"value")?;
    assert_eq!(db.get("test")?, Some(b"value".to_vec()));
    
    Ok(())
}

#[test]
fn test_working_with_strings() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    
    let mut db = SnailDb::open(&db_path)?;
    
    // Store string values
    db.put("name", "snaildb")?;
    db.put("version", "0.1.0")?;
    
    // Retrieve as string
    if let Some(bytes) = db.get("name")? {
        let value = String::from_utf8_lossy(&bytes);
        assert_eq!(value, "snaildb");
    } else {
        panic!("Key 'name' should exist");
    }
    
    if let Some(bytes) = db.get("version")? {
        let value = String::from_utf8_lossy(&bytes);
        assert_eq!(value, "0.1.0");
    } else {
        panic!("Key 'version' should exist");
    }
    
    Ok(())
}

#[test]
fn test_error_handling() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    
    use anyhow::Context;
    
    let mut db = SnailDb::open(&db_path)
        .context("Failed to open database")?;
    
    db.put("key", "value")
        .context("Failed to store key-value pair")?;
    
    // Verify it was stored
    assert_eq!(db.get("key")?, Some(b"value".to_vec()));
    
    Ok(())
}

#[test]
fn test_multiple_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    
    let mut db = SnailDb::open(&db_path)?;
    
    // Put multiple values
    for i in 0..10 {
        db.put(&format!("key:{}", i), format!("value:{}", i).as_bytes())?;
    }
    
    // Retrieve all values
    for i in 0..10 {
        let key = format!("key:{}", i);
        let expected_value = format!("value:{}", i);
        match db.get(&key)? {
            Some(value) => {
                let value_str = String::from_utf8_lossy(&value);
                assert_eq!(value_str, expected_value);
            }
            None => panic!("Key {} should exist", key),
        }
    }
    
    // Delete some keys
    db.delete("key:5")?;
    db.delete("key:7")?;
    
    // Verify deletions
    assert_eq!(db.get("key:5")?, None);
    assert_eq!(db.get("key:7")?, None);
    
    // Verify others still exist
    assert_eq!(db.get("key:0")?, Some(b"value:0".to_vec()));
    assert_eq!(db.get("key:9")?, Some(b"value:9".to_vec()));
    
    Ok(())
}
