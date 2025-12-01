mod utils;
mod lsm;
mod memtable;
mod sstable;
mod value;
mod wal;

use anyhow::Result;
use lsm::LsmTree;
use std::io::{self, Write};

fn main() -> Result<()> {
    let mut tree = LsmTree::open("data")?.with_flush_threshold(64); // flush threshold is the number of entries to flush to the sstable (disk) from memtable (memory)
    println!("--------------------------------");
    println!("tree: {:?}", tree);
    println!("--------------------------------");
    println!();
    println!("LSM store. Commands: PUT key value | GET key | DEL key | MEMTABLE | WAL | COUNT | FLUSH | EXIT");

    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        line.clear();
        print!("db> ");
        io::stdout().flush()?;

        if stdin.read_line(&mut line)? == 0 {
            break;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_lowercase().as_str() {
            "put" => {
                if parts.len() < 3 {
                    println!("usage: PUT key value");
                    continue;
                }
                let key = parts[1];
                let value = parts[2];
                tree.put(key.to_string(), value.as_bytes().to_vec())?;
                println!("ok");
            }
            "get" => {
                if parts.len() < 2 {
                    println!("usage: GET key");
                    continue;
                }
                let key = parts[1];
                match tree.get(key)? {
                    Some(value) => println!("{}", String::from_utf8_lossy(&value)),
                    None => println!("<nil>"),
                }
            }
            "del" => {
                if parts.len() < 2 {
                    println!("usage: DEL key");
                    continue;
                }
                let key = parts[1];
                tree.delete(key)?;
                println!("ok");
            }
            "memtable" => println!("memtable: {:?}", tree.memtable),
            "wal" => println!("wal: {:?}", tree.wal),
            "count" => println!("sstables count: {:?}", tree.sstables.len()),
            "flush" => tree.flush_memtable()?,
            "exit" | "quit" => break,
            other => println!("{other} commands are not supported"),
        }
    }
    Ok(())
}
