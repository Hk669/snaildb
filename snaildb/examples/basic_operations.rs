use snaildb::SnailDb;
use anyhow::Result;

fn main() -> Result<()> {
    // Open or create a database at a directory
    let mut db = SnailDb::open("./data")?;
    
    // Store a key-value pair
    db.put("user:1", b"Alice")?;
    db.put("user:2", b"Bob")?;
    
    // Retrieve a value
    match db.get("user:1")? {
        Some(value) => println!("Found: {:?}", String::from_utf8_lossy(&value)),
        None => println!("Key not found"),
    }
    
    // Delete a key
    db.delete("user:2")?;
    
    // Verify deletion
    match db.get("user:2")? {
        Some(_) => println!("Key still exists (unexpected)"),
        None => println!("Key successfully deleted"),
    }
    
    Ok(())
}

