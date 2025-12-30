# Examples

This directory contains example programs demonstrating how to use snaildb.

## Running Examples

You can run any example using Cargo:

```bash
cargo run --example <example_name> --package snaildb
```

## Available Examples

### `basic_operations`
Demonstrates basic database operations:
- Opening a database
- Storing key-value pairs with `put()`
- Retrieving values with `get()`
- Deleting keys with `delete()`

Run with:
```bash
cargo run --example basic_operations --package snaildb
```

### `string_operations`
Shows how to work with string values:
- Storing string values directly (no need to convert to bytes)
- Retrieving and converting bytes back to strings
- Using `String::from_utf8_lossy()` for safe conversion

Run with:
```bash
cargo run --example string_operations --package snaildb
```

### `custom_flush_threshold`
Demonstrates configuring a custom flush threshold:
- Setting a custom memtable flush threshold
- Understanding when data gets flushed to disk

Run with:
```bash
cargo run --example custom_flush_threshold --package snaildb
```

### `batch_operations`
Demonstrates batch operations:
- Storing multiple key-value pairs in a loop
- Retrieving multiple values
- Batch deletions
- Verifying operations

Run with:
```bash
cargo run --example batch_operations --package snaildb
```

## Note

All examples create a `./data` directory in the current working directory. You may want to clean this up after running examples, or modify the examples to use a temporary directory.
