<img width="2266" height="642" alt="image" src="https://github.com/user-attachments/assets/53e68713-afb5-42cc-af44-a0ca89d27121" />


<a href="https://crates.io/crates/snaildb">![Crates.io Version](https://img.shields.io/crates/v/snaildb?style=flat-square)</a>
![GitHub License](https://img.shields.io/github/license/Hk669/snaildb?style=flat-square)
<a href="https://github.com/Hk669/snaildb">![GitHub](https://img.shields.io/github/stars/Hk669/snaildb?style=flat-square)</a>
<a href="https://docs.rs/snaildb/latest/snaildb/">![Docs](https://img.shields.io/badge/docs-docs.rs-blue?style=flat-square)</a>

An embedded, persistent key-value store written in Rust.

## Principles

- **High write throughput** - Optimized for write-heavy workloads
- **Durability** - Write-ahead logging (WAL) ensures data persistence

## Features

- LSM-tree based storage engine
- Write-ahead logging (WAL) for durability
- Embedded library for use in Rust applications
- Thread-safe operations

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
snaildb = "0.1"
```

Or install the binaries from [GitHub Releases](https://github.com/Hk669/snaildb/releases).

## Usage

### Library Usage (snaildb)

#### Basic Operations

```rust
use snaildb::engine::lsm::LsmTree;
use anyhow::Result;

fn main() -> Result<()> {
    // Open or create a database at a directory
    let mut db = LsmTree::open("./data")?;
    
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
    
    Ok(())
}
```

#### Custom Flush Threshold

```rust
use snaildb::engine::lsm::LsmTree;

let mut db = LsmTree::open("./data")?
    .with_flush_threshold(256); // Flush memtable after 256 entries
```

#### Working with Strings

```rust
let mut db = LsmTree::open("./data")?;

// Store string values
db.put("name", "snaildb")?;
db.put("version", "0.1.0")?;

// Retrieve as string
if let Some(bytes) = db.get("name")? {
    let value = String::from_utf8_lossy(&bytes);
    println!("Name: {}", value);
}
```

#### Error Handling

```rust
use snaildb::engine::lsm::LsmTree;
use anyhow::{Result, Context};

fn store_data() -> Result<()> {
    let mut db = LsmTree::open("./data")
        .context("Failed to open database")?;
    
    db.put("key", "value")
        .context("Failed to store key-value pair")?;
    
    Ok(())
}
```

## Examples

See the [examples directory](./examples) for more detailed usage examples.

## Configuration

### Flush Threshold

The flush threshold determines when the in-memory memtable is flushed to disk as an SSTable. Default is 128 entries.

```rust
let mut db = LsmTree::open("./data")?
    .with_flush_threshold(256); // Custom threshold
```

## Architecture

snailDB uses an LSM-tree (Log-Structured Merge-tree) architecture:

1. **Memtable** - In-memory structure for recent writes
2. **WAL (Write-Ahead Log)** - Ensures durability by logging all writes
3. **SSTables** - Immutable on-disk structures created from flushed memtables

## Roadmap

Current work in progress:

- **Durable async WAL** - Group commit and crash recovery improvements
- **Object storage support** - S3/MinIO integration for cloud-native deployments
- **Compaction** - Background compaction to manage SSTable growth
- **Iterators & scans** - Efficient range queries and prefix scans
- **Performance optimizations** - Block caching and metrics

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Links

- [GitHub Repository](https://github.com/Hk669/snaildb)
- [Crates.io](https://crates.io/crates/snaildb)
- [Documentation](https://docs.rs/snaildb)
