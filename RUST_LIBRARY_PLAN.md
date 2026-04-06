# Rust-focused Cobrix extraction plan (no Spark)

This note maps the current Cobrix modules to a Rust-native library design for users who need:

- COBOL copybook parsing
- binary data record reading
- zero Spark dependency
- integration into long-running services (for example a TCP server on z/OS)

## 1) What to keep from Cobrix

From this repository, the reusable pieces are already concentrated in `cobol-parser`:

- `za.co.absa.cobrix.cobol.parser.CopybookParser` parses copybook text into AST structures.
- `za.co.absa.cobrix.cobol.reader.extractors.raw.*` handles fixed/variable-length raw record extraction.
- `za.co.absa.cobrix.cobol.reader.extractors.record.RecordExtractors` maps bytes into typed values.
- `za.co.absa.cobrix.cobol.reader.parameters.ReaderParameters` and parser/validator classes define reader options.

The Spark-specific logic lives in `spark-cobol` and can be ignored for a Rust port.

## 2) Recommended Rust crate split

Use a small workspace with clear boundaries:

1. `copybook-core`
   - tokenizer + parser
   - AST + semantic validation (REDEFINES, OCCURS DEPENDING ON)

2. `cobol-codec`
   - EBCDIC/ASCII decoding, COMP/COMP-3 numeric unpacking
   - conversion functions used by record decoding

3. `record-reader`
   - fixed-length, VB/RDW, and custom header extractors
   - iterator/stream interface over `Read`/`AsyncRead`

4. `record-decoder`
   - applies copybook schema to raw bytes
   - emits row-oriented values (or Arrow arrays/builders)

5. `cobol-arrow` (optional)
   - Arrow schema derivation + `RecordBatch` output
   - usable by Arrow Flight server implementation

## 3) Public API shape (service-friendly)

Expose a narrow API to make TCP integration simple:

```rust
pub struct ParserConfig { /* dialect and formatting options */ }
pub struct ReaderConfig { /* record format, encoding, trimming, etc. */ }

pub fn parse_copybook(copybook_text: &str, cfg: &ParserConfig) -> Result<Schema>;
pub fn stream_rows<R: std::io::Read>(input: R, schema: &Schema, cfg: &ReaderConfig)
    -> impl Iterator<Item = Result<Row>>;
```

For async network services, add:

```rust
pub fn stream_rows_async<R: tokio::io::AsyncRead + Unpin>(...)
    -> impl Stream<Item = Result<Row>>;
```

## 4) z/OS and transport guidance

- Keep the parsing/decoding crate independent of any network server framework.
- Put protocol output adapters in separate crates:
  - Arrow Flight adapter (`arrow-flight`)
  - MySQL wire adapter (`opensrv-mysql` or equivalent)
- This lets your z/OS TCP server select protocol at runtime while sharing one decode pipeline.

## 5) Migration strategy from current codebase

1. Start with copybook parser parity tests against `data/*.cob` fixtures.
2. Port raw extractors next (fixed, then variable block/RDW).
3. Port primitive value decoding and signed/packed decimal edge cases.
4. Add hierarchical/multi-segment decoding once flat records pass parity.
5. Add Arrow conversion as final step.

## 6) Test corpus recommendations

Reuse existing fixtures in `data/` for compatibility testing:

- fixed-length files
- variable-length files with headers
- special characters / display numeric cases
- hierarchical and multisegment examples

Success criterion: Rust output matches Cobrix `cobol-parser` behavior for equivalent options.

## 7) Practical first milestone

Deliver one crate that supports:

- copybook parse
- fixed-length record extraction
- flat row decode
- `Iterator<Result<Row>>` API

Then integrate with your TCP server and expose one protocol first (Arrow Flight is typically easier for typed analytics; MySQL wire can be added after the decode model stabilizes).

### Status in this repository

- ✅ Initial implementation is available in `rust-cobol/`.
- ✅ Includes copybook parsing + fixed-length streaming iterator + row output model.
- ⏳ Still pending full decode parity for all binary/packed formats and variable-block extractors.
