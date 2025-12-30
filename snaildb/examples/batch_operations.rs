use snaildb::SnailDb;
use anyhow::Result;

fn main() -> Result<()> {
    let mut db = SnailDb::open("./data")?;
    
    println!("Storing multiple key-value pairs...");
    
    // Put multiple values
    for i in 0..10 {
        let key = format!("key:{}", i);
        let value = format!("value:{}", i);
        db.put(&key, value.as_bytes())?;
        println!("Stored: {} -> {}", key, value);
    }
    
    println!("\nRetrieving all values...");
    
    // Retrieve all values
    for i in 0..10 {
        let key = format!("key:{}", i);
        match db.get(&key)? {
            Some(value) => {
                let value_str = String::from_utf8_lossy(&value);
                println!("Retrieved: {} -> {}", key, value_str);
            }
            None => println!("Key {} not found", key),
        }
    }
    
    println!("\nDeleting some keys...");
    
    // Delete some keys
    db.delete("key:5")?;
    db.delete("key:7")?;
    println!("Deleted key:5 and key:7");
    
    // Verify deletions
    assert_eq!(db.get("key:5")?, None);
    assert_eq!(db.get("key:7")?, None);
    println!("Verified deletions successful");
    
    // Verify others still exist
    assert_eq!(db.get("key:0")?, Some(b"value:0".to_vec()));
    assert_eq!(db.get("key:9")?, Some(b"value:9".to_vec()));
    println!("Verified remaining keys still exist");
    
    Ok(())
}

