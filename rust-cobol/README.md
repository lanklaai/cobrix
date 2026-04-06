# cobrix-rust (initial milestone)

This folder contains the first executable step of `RUST_LIBRARY_PLAN.md`.

Implemented now:

- Copybook parsing for a broader subset: nested levels, continuation lines, `REDEFINES`, `OCCURS`, `DEPENDING ON`, `PIC X`, `PIC 9`, signed numerics, implied decimal `V`, explicit decimal point syntax, and usage detection (`DISPLAY`, `COMP`, `COMP-1/2/3/4/5`, `BINARY`)
- Fixed-length record extraction from any `Read`
- Row decoding to a simple typed model (`Value::Text`, `Value::Number`, `Value::Bytes`)
- Iterator API: `stream_rows(...) -> impl Iterator<Item = Result<Row>>`

Current limitations (planned follow-up):

- Binary and packed numeric fields are currently surfaced as `Value::Bytes` (parse support exists, semantic decode still pending)
- Full parity features such as advanced REDEFINES materialization and variable-block/RDW extraction are not yet implemented
- No EBCDIC transcoding yet (bytes are currently interpreted as UTF-8/ASCII lossily for DISPLAY text/numbers)

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
