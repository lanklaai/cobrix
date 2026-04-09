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
                    return None;
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
    let rdw_little_endian_header = schema.fields.len() >= 2
        && schema.fields[0].picture.as_ref().is_some_and(|pic| {
            matches!(
                pic.usage,
                Usage::Comp | Usage::Comp4 | Usage::Comp5 | Usage::Binary
            )
        })
        && schema.fields[0].byte_len() == 2
        && schema.fields[1]
            .picture
            .as_ref()
            .is_some_and(Picture::is_alphanumeric)
        && schema.fields[1].byte_len() == 2
        && record.len() >= 4
        && record[0] == 0
        && record[1] == 0
        && record[3] == 0;

    for (index, field) in schema.fields.iter().enumerate() {
        let len = field.byte_len();
        let raw = &record[offset..offset + len];
        let decode_bytes = if rdw_little_endian_header && index == 0 {
            &record[2..4]
        } else {
            raw
        };
        offset += len;

        let picture = field.picture.as_ref().expect("leaf fields only");
        buffer.clear();
        buffer.extend_from_slice(decode_bytes);

        let value = decode_field(decode_bytes, &mut buffer, picture, cfg).map_err(|message| {
            CobolError::Decode {
                field: field.name.clone(),
                message,
            }
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
    if matches!(picture.usage, Usage::Comp3) {
        return Ok(
            decode_comp3(bytes, picture).unwrap_or_else(|_| Value::Number(comp3_zero(picture)))
        );
    }
    if matches!(
        picture.usage,
        Usage::Comp | Usage::Comp4 | Usage::Comp5 | Usage::Binary
    ) {
        return decode_binary(bytes, picture);
    }

    let bytes = if cfg.format.is_ebcdic() {
        Ebcdic::ebcdic_to_ascii(bytes, buffer, bytes.len(), false, true);
        buffer
    } else {
        bytes
    };
    let raw = String::from_utf8_lossy(bytes).to_string();

    if picture.is_alphanumeric() {
        if cfg.trim_text {
            return Ok(Value::Text(
                raw.trim_end_matches(['\u{0}', ' ']).to_string(),
            ));
        }
        return Ok(Value::Text(raw));
    }

    match picture.usage {
        Usage::Display => {
            let normalized = raw.trim();
            if normalized.is_empty() {
                return Ok(Value::Number("0".to_string()));
            }
            Ok(Value::Number(normalized.to_string()))
        }
        _ => Ok(Value::Bytes(bytes.to_vec())),
    }
}

fn decode_binary(bytes: &[u8], picture: &Picture) -> std::result::Result<Value, String> {
    let mut raw = match bytes.len() {
        2 => i64::from(i16::from_le_bytes([bytes[0], bytes[1]])),
        4 => i64::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
        8 => i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]),
        _ => {
            return Err(format!(
                "unsupported COMP/BINARY payload length: {}",
                bytes.len()
            ));
        }
    };

    if !picture.signed && raw < 0 {
        raw = match bytes.len() {
            2 => i64::from(u16::from_le_bytes([bytes[0], bytes[1]])),
            4 => i64::from(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
            8 => u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]) as i64,
            _ => raw,
        };
    }

    let value = if picture.digits_after == 0 {
        raw.to_string()
    } else {
        let negative = raw < 0;
        let abs = raw.unsigned_abs().to_string();
        let mut digits = abs;
        if digits.len() <= picture.digits_after {
            let mut padded = "0".repeat(picture.digits_after + 1 - digits.len());
            padded.push_str(&digits);
            digits = padded;
        }
        let split = digits.len() - picture.digits_after;
        let mut rendered = String::with_capacity(digits.len() + 1 + usize::from(negative));
        if negative {
            rendered.push('-');
        }
        rendered.push_str(&digits[..split]);
        rendered.push('.');
        rendered.push_str(&digits[split..]);
        rendered
    };

    Ok(Value::Number(value))
}

fn comp3_zero(picture: &Picture) -> String {
    if picture.digits_after == 0 {
        return "0".to_string();
    }

    let mut value = String::with_capacity(2 + picture.digits_after);
    value.push('0');
    value.push('.');
    value.extend(std::iter::repeat_n('0', picture.digits_after));
    value
}

fn decode_comp3(bytes: &[u8], picture: &Picture) -> std::result::Result<Value, String> {
    if bytes.is_empty() {
        return Err("empty COMP-3 payload".to_string());
    }

    let mut digits = Vec::with_capacity(bytes.len() * 2);
    for byte in bytes.iter().take(bytes.len() - 1) {
        digits.push((byte >> 4) & 0x0F);
        digits.push(byte & 0x0F);
    }

    let last = bytes[bytes.len() - 1];
    digits.push((last >> 4) & 0x0F);
    let sign_nibble = last & 0x0F;

    let negative = if picture.signed {
        match sign_nibble {
            0x0B | 0x0D => true,
            0x0A | 0x0C | 0x0E | 0x0F => false,
            0x00..=0x09 => {
                digits.push(sign_nibble);
                false
            }
            _ => return Err(format!("invalid COMP-3 sign nibble: {sign_nibble:#x}")),
        }
    } else {
        digits.push(sign_nibble);
        false
    };

    let expected_digits = picture.digits_before + picture.digits_after;
    if digits.iter().any(|digit| *digit > 9) {
        return Err("invalid COMP-3 digit nibble".to_string());
    }

    if digits.len() > expected_digits {
        let drop = digits.len() - expected_digits;
        if digits[..drop].iter().any(|digit| *digit != 0) {
            return Err("COMP-3 payload has more digits than picture allows".to_string());
        }
        digits.drain(..drop);
    } else if digits.len() < expected_digits {
        let mut padded = vec![0u8; expected_digits - digits.len()];
        padded.extend(digits);
        digits = padded;
    }

    let mut value = String::with_capacity(expected_digits + usize::from(picture.digits_after > 0));
    for (idx, digit) in digits.into_iter().enumerate() {
        if picture.digits_after > 0 && idx == picture.digits_before {
            value.push('.');
        }
        value.push(char::from(b'0' + digit));
    }

    if negative && value.chars().any(|ch| ch != '0' && ch != '.') {
        value.insert(0, '-');
    }

    Ok(Value::Number(value))
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
        let cfg = DecodeConfig {
            format: Format::Ascii,
            ..Default::default()
        };
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &cfg)
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].1, Value::Number("0001".into()));
        assert_eq!(rows[0][1].1, Value::Text("ALICE".into()));
        assert_eq!(rows[1][1].1, Value::Text("BOB".into()));
    }

    #[test]
    fn exposes_comp3_as_raw_bytes_for_now() {
        let schema = parse_copybook("01 REC.\n05 ID PIC S9(5) COMP-3.", &ParserConfig::default())
            .expect("schema");

        let data = [0x12_u8, 0x34, 0x5C];
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &DecodeConfig::default())
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows[0][0].1, Value::Number("12345".into()));
    }

    #[test]
    fn decodes_signed_comp3_with_implied_decimal() {
        let schema = parse_copybook(
            "01 REC.\n05 BAL PIC S9(5)V99 COMP-3.",
            &ParserConfig::default(),
        )
        .expect("schema");

        let data = [0x12_u8, 0x34, 0x56, 0x7D];
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &DecodeConfig::default())
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows[0][0].1, Value::Number("-12345.67".into()));
    }

    #[test]
    fn decodes_zero_comp3_with_null_sign_nibble() {
        let schema = parse_copybook(
            "01 REC.
05 AMT PIC S9(5) COMP-3.",
            &ParserConfig::default(),
        )
        .expect("schema");

        let data = [0x00_u8, 0x00, 0x00];
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &DecodeConfig::default())
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows[0][0].1, Value::Number("00000".into()));
    }

    #[test]
    fn keeps_non_digit_display_payloads_as_numbers() {
        let schema =
            parse_copybook("01 REC.\n05 CODE PIC 9(2).", &ParserConfig::default()).expect("schema");

        let cfg = DecodeConfig {
            trim_text: true,
            format: Format::Ascii,
        };

        let rows: Vec<Row> = stream_rows(&b"SA"[..], &schema, &cfg)
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows[0][0].1, Value::Number("SA".into()));
    }

    #[test]
    fn can_parse_transaction_comp3() {
        let cb1 = include_str!("../data/TRANSHST.cbl");
        let schema = parse_copybook(cb1, &ParserConfig::default()).expect("schema");

        let cfg = DecodeConfig {
            trim_text: true,
            ..Default::default()
        };

        let data = include_bytes!("../data/TRANSACTIONS.ebcdic");
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &cfg)
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert!(!rows.is_empty());
        assert_eq!(rows[0][0].1, Value::Text("TXN0000000000001".into()));

        let first_item_amt = rows[0]
            .iter()
            .find(|(name, _)| name == "TH-ITEM-AMT")
            .map(|(_, value)| value)
            .expect("TH-ITEM-AMT field");
        assert!(matches!(first_item_amt, Value::Number(_)));
    }

    #[test]
    fn test_data_not_garbage() {
        let cb1 = include_str!("../../data/test5d_copybook.cob");
        let schema = parse_copybook(cb1, &ParserConfig::default()).expect("schema");

        let cfg = DecodeConfig {
            trim_text: true,
            ..Default::default()
        };

        let data = include_bytes!("../../data/test5_data/COMP.DETAILS.SEP30.DATA.dat");
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &cfg)
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert!(!rows.is_empty());
        // Record length should not be a string
        assert_eq!(rows[0][0].1, Value::Number("64".to_string()));
        // Should not have a bunch of nulls at the end
        assert_eq!(rows[0][2].1, Value::Text("C".into()));
        assert_eq!(rows[0][4].1, Value::Text("Joan Q & Z".into()));
        // Should not be null terminated
        assert_eq!(rows[0][5].1, Value::Text("10 Sandton, Johannesburg".into()));
    }

    #[test]
    fn test_data_not_garbage2() {
        let cb1 = include_str!("../../data/test5d_copybook.cob");
        let schema = parse_copybook(cb1, &ParserConfig::default()).expect("schema");

        let cfg = DecodeConfig {
            trim_text: true,
            ..Default::default()
        };

        let data = include_bytes!("../../data/test5_data/COMP.DETAILS.SEP30.DATA.dat");
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &cfg)
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert!(!rows.is_empty());
        // Record length should not be a string
        assert_eq!(rows[1][0].1, Value::Number("60".to_string()));
        // Should not have a bunch of nulls at the end
        assert_eq!(rows[1][2].1, Value::Text("P".into()));
        assert_eq!(rows[1][4].1, Value::Text("+(277) 944 44 5".into()));
        // Should not be null terminated
        assert_eq!(rows[1][5].1, Value::Text("5 Janiece Newcombe".into()));
    }

    #[test]
    fn decodes_comp_and_trims_null_terminated_text() {
        let schema = parse_copybook(
            "01 REC.
05 NUM PIC 9(4) COMP.
05 TXT PIC X(5).",
            &ParserConfig::default(),
        )
        .expect("schema");

        let data = [0x40_u8, 0x00, 0xC3, 0xD6, 0xC2, 0x00, 0x00];
        let rows: Vec<Row> = stream_rows(&data[..], &schema, &DecodeConfig::default())
            .collect::<Result<Vec<_>>>()
            .expect("rows");

        assert_eq!(rows[0][0].1, Value::Number("64".into()));
        assert_eq!(rows[0][1].1, Value::Text("COB".into()));
    }
}
