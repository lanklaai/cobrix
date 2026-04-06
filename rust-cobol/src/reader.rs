use std::io::Read;

use crate::schema::{Picture, Row, Schema, Usage, Value};
use ebcdic::ebcdic::Ebcdic;

use crate::{CobolError, Result};

#[derive(Debug, Clone)]
pub struct DecodeConfig {
    pub trim_text: bool,
    pub format: Format,
}

#[derive(Debug, Default, Clone, Copy)]
pub enum Format {
    #[default]
    Ebcdic,
    Ascii,
}

impl Format {
    pub fn is_ebcdic(&self) -> bool {
        matches!(self, Self::Ebcdic)
    }
}

impl Default for DecodeConfig {
    fn default() -> Self {
        Self {
            trim_text: true,
            format: Format::default(),
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
    let mut buffer = vec![];

    for field in &schema.fields {
        let len = field.byte_len();
        let raw = &record[offset..offset + len];
        offset += len;

        let picture = field.picture.as_ref().expect("leaf fields only");
        buffer.clear();
        buffer.extend_from_slice(raw);

        let value =
            decode_field(raw, &mut buffer, picture, cfg).map_err(|message| CobolError::Decode {
                field: field.name.clone(),
                message,
            })?;

        row.push((field.name.clone(), value));
    }

    Ok(row)
}

fn decode_field(
    bytes: &[u8],
    buffer: &mut [u8],
    picture: &Picture,
    cfg: &DecodeConfig,
) -> std::result::Result<Value, String> {
    let bytes = if cfg.format.is_ebcdic() {
        Ebcdic::ebcdic_to_ascii(bytes, buffer, bytes.len(), false, true);
        buffer
    } else {
        bytes
    };
    let raw = String::from_utf8_lossy(bytes).to_string();

    if picture.is_alphanumeric() {
        if cfg.trim_text {
            return Ok(Value::Text(raw.trim_end().to_string()));
        }
        return Ok(Value::Text(raw));
    }

    match picture.usage {
        Usage::Display => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ParserConfig, parse_copybook};

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
}
