use std::io::Read;

use crate::schema::{Picture, Row, Schema, Value};
use crate::{CobolError, Result};

#[derive(Debug, Clone)]
pub struct DecodeConfig {
    pub trim_text: bool,
}

impl Default for DecodeConfig {
    fn default() -> Self {
        Self { trim_text: true }
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
    let raw = String::from_utf8_lossy(bytes).to_string();
    if picture.is_alphanumeric() {
        if cfg.trim_text {
            return Ok(Value::Text(raw.trim_end().to_string()));
        }
        return Ok(Value::Text(raw));
    }

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
}
