# cobrix-rust (initial milestone)

This folder contains the first executable step of `RUST_LIBRARY_PLAN.md`.

Implemented now:

- Copybook parsing for a practical subset (`PIC X(...)`, `PIC 9(...)`, signed + implied decimal `V`, `OCCURS n`)
- Fixed-length record extraction from any `Read`
- Row decoding to a simple typed model (`Value::Text`, `Value::Number`)
- Iterator API: `stream_rows(...) -> impl Iterator<Item = Result<Row>>`

Current limitations (planned follow-up):

- No COMP / COMP-3 binary decoding yet
- No REDEFINES materialization yet
- No variable-block / RDW record extractors yet
- No EBCDIC transcoding yet (bytes are currently interpreted as UTF-8/ASCII lossily)

## Quick example

```rust
use cobrix_rust::{parse_copybook, stream_rows, DecodeConfig, ParserConfig};

let copybook = "01 REC.\n05 ID PIC 9(4).\n05 NAME PIC X(10).";
let schema = parse_copybook(copybook, &ParserConfig::default())?;
let bytes = b"0001ALICE     0002BOB       ";

for row in stream_rows(&bytes[..], &schema, &DecodeConfig::default()) {
    println!("{:?}", row?);
}
# Ok::<(), Box<dyn std::error::Error>>(())
```
