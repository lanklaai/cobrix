use std::io::Read;

use ebcdic::ebcdic::Ebcdic;

use crate::schema::{Picture, Row, Schema, Usage, Value};
use crate::{CobolError, Result};

#[derive(Debug, Clone, Copy)]
pub enum TextEncoding {
    Auto,
    Utf8,
    Ebcdic,
}

#[derive(Debug, Clone)]
pub struct DecodeConfig {
    pub trim_text: bool,
    pub text_encoding: TextEncoding,
}

impl Default for DecodeConfig {
    fn default() -> Self {
        Self {
            trim_text: true,
            text_encoding: TextEncoding::Auto,
        }
    }
}

pub struct FixedRecordReader<R: Read> {
    input: R,
    schema: Schema,
    cfg: DecodeConfig,
    done: bool,
}

impl<R: Read> FixedRecordReader<R> {
    pub fn new(input: R, schema: Schema, cfg: DecodeConfig) -> Self {
        Self {
            input,
            schema,
            cfg,
            done: false,
        }
    }
}

impl<R: Read> Iterator for FixedRecordReader<R> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let len = self.schema.fixed_record_len();
        let mut buf = vec![0u8; len];
        let mut read_total = 0usize;

        while read_total < len {
            match self.input.read(&mut buf[read_total..]) {
                Ok(0) => {
                    if read_total == 0 {
                        self.done = true;
                        return None;
                    }
                    self.done = true;
                    return Some(Err(CobolError::Decode {
                        field: "<record>".to_string(),
                        message: format!(
                            "partial record encountered: expected {} bytes, got {}",
                            len, read_total
                        ),
                    }));
                }
                Ok(n) => read_total += n,
                Err(err) => {
                    self.done = true;
                    return Some(Err(CobolError::Io(err)));
                }
            }
        }

        Some(decode_row(&buf, &self.schema, &self.cfg))
    }
}

pub fn stream_rows<R: Read>(
    input: R,
    schema: &Schema,
    cfg: &DecodeConfig,
) -> impl Iterator<Item = Result<Row>> {
    FixedRecordReader::new(input, schema.clone(), cfg.clone())
}

fn decode_row(record: &[u8], schema: &Schema, cfg: &DecodeConfig) -> Result<Row> {
    let mut offset = 0usize;
    let mut row = Vec::with_capacity(schema.fields.len());

    for field in &schema.fields {
        let len = field.byte_len();
        let raw = &record[offset..offset + len];
        offset += len;

        let picture = field.picture.as_ref().expect("leaf fields only");
        let value = decode_field(raw, picture, cfg).map_err(|message| CobolError::Decode {
            field: field.name.clone(),
            message,
        })?;

        row.push((field.name.clone(), value));
    }

    Ok(row)
}

fn decode_field(
    bytes: &[u8],
    picture: &Picture,
    cfg: &DecodeConfig,
) -> std::result::Result<Value, String> {
    if picture.is_alphanumeric() {
        let raw = decode_text(bytes, cfg.text_encoding);
        if cfg.trim_text {
            return Ok(Value::Text(raw.trim_end().to_string()));
        }
        return Ok(Value::Text(raw));
    }

    match picture.usage {
        Usage::Display => {
            let raw = decode_text(bytes, cfg.text_encoding);
            let normalized = raw.trim();
            if normalized.is_empty() {
                return Ok(Value::Number("0".to_string()));
            }

            if normalized
                .chars()
                .all(|c| c.is_ascii_digit() || c == '+' || c == '-')
            {
                return Ok(Value::Number(normalized.to_string()));
            }

            Err(format!("unsupported numeric payload '{normalized}'"))
        }
        _ => Ok(Value::Bytes(bytes.to_vec())),
    }
}

fn decode_text(bytes: &[u8], encoding: TextEncoding) -> String {
    match encoding {
        TextEncoding::Auto => {
            let utf8 = String::from_utf8_lossy(bytes).to_string();
            if is_mostly_printable(&utf8) {
                utf8
            } else {
                decode_ebcdic(bytes)
            }
        }
        TextEncoding::Utf8 => String::from_utf8_lossy(bytes).to_string(),
        TextEncoding::Ebcdic => decode_ebcdic(bytes),
    }
}

fn decode_ebcdic(bytes: &[u8]) -> String {
    let mut ascii = vec![0_u8; bytes.len()];
    Ebcdic::ebcdic_to_ascii(bytes, &mut ascii, bytes.len(), false, true);
    String::from_utf8_lossy(&ascii).to_string()
}

fn is_mostly_printable(text: &str) -> bool {
    if text.is_empty() {
        return true;
    }
    let printable = text
        .chars()
        .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
        .count();
    printable * 10 >= text.chars().count() * 8
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_copybook, ParserConfig};

    #[test]
    fn reads_fixed_rows() {
        let schema = parse_copybook(
            "01 REC.\n05 ID PIC 9(4).\n05 NAME PIC X(5).",
            &ParserConfig::default(),
        )
        .expect("schema");

        let data = b"0001ALICE0002BOB  ";
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &DecodeConfig::default())
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].1, Value::Number("0001".into()));
        assert_eq!(rows[0][1].1, Value::Text("ALICE".into()));
        assert_eq!(rows[1][1].1, Value::Text("BOB".into()));
    }

    #[test]
    fn exposes_comp3_as_raw_bytes_for_now() {
        let schema = parse_copybook("01 REC.\n05 ID PIC 9(5) COMP-3.", &ParserConfig::default())
            .expect("schema");

        let data = [0x12_u8, 0x34, 0x5C];
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &DecodeConfig::default())
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows[0][0].1, Value::Bytes(vec![0x12, 0x34, 0x5C]));
    }

    #[test]
    fn decodes_ebcdic_text_and_display_numbers() {
        let schema = parse_copybook(
            "01 REC.\n05 ID PIC 9(4).\n05 NAME PIC X(5).",
            &ParserConfig::default(),
        )
        .expect("schema");

        let mut bytes = [0_u8; 9];
        Ebcdic::ascii_to_ebcdic(b"0001ALICE", &mut bytes, 9, true);

        let cfg = DecodeConfig {
            trim_text: true,
            text_encoding: TextEncoding::Ebcdic,
        };
        let rows: Vec<Row> = stream_rows(&bytes[..], &schema, &cfg)
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows[0][0].1, Value::Number("0001".into()));
        assert_eq!(rows[0][1].1, Value::Text("ALICE".into()));
    }
}
