# cobrix-rust-datafusion

DataFusion backend crate for `cobrix-rust`.

This crate lets you register COBOL data files (decoded via `cobrix-rust`) as a DataFusion `MemTable`
so they can be queried with SQL.

## What it does

- Parses copybook text using `cobrix-rust`
- Streams records from an input `Read`
- Converts decoded rows into Arrow `RecordBatch`
- Registers a DataFusion table (`register_cobol_table`)

## Example

```rust
use cobrix_rust_datafusion::{register_cobol_table, BackendConfig};
use datafusion::prelude::SessionContext;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let ctx = SessionContext::new();
let copybook = "01 REC.\n05 ID PIC 9(4).\n05 NAME PIC X(5).";
let data = b"0001ALICE0002BOB  ";

register_cobol_table(&ctx, "records", copybook, &data[..], &BackendConfig::default())?;
let df = ctx.sql("SELECT * FROM records ORDER BY ID").await?;
let _batches = df.collect().await?;
# Ok(()) }
```
