use snaildb::SnailDb;
use anyhow::Result;

fn main() -> Result<()> {
    let mut db = SnailDb::open("./data")?;

    // Store string values directly
    db.put("name", "snaildb")?;
    db.put("version", "0.2.0")?;
    db.put("description", "An embedded, persistent key-value store")?;

    // Retrieve as string
    if let Some(bytes) = db.get("name")? {
        let value = String::from_utf8_lossy(&bytes);
        println!("Name: {}", value);
    }

    if let Some(bytes) = db.get("version")? {
        let value = String::from_utf8_lossy(&bytes);
        println!("Version: {}", value);
    }

    if let Some(bytes) = db.get("description")? {
        let value = String::from_utf8_lossy(&bytes);
        println!("Description: {}", value);
    }

    Ok(())
}

