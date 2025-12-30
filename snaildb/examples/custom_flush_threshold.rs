use snaildb::SnailDb;
use anyhow::Result;

fn main() -> Result<()> {
    // Create a database with a custom flush threshold
    // The memtable will be flushed to disk when it reaches 256 MiB
    let mut db = SnailDb::open("./data")?
        .with_flush_threshold(256 * 1024 * 1024); // Flush memtable after 256 MiB
    
    // Use the database normally
    db.put("config:flush_threshold", "256 MiB")?;
    
    match db.get("config:flush_threshold")? {
        Some(value) => {
            let value_str = String::from_utf8_lossy(&value);
            println!("Flush threshold configured: {}", value_str);
        }
        None => println!("Key not found"),
    }
    
    Ok(())
}

