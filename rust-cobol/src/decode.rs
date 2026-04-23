use crate::Picture;
use crate::Value;
use error::DecodeError;

pub mod error;

pub fn decode_comp3(bytes: &[u8], picture: &Picture) -> std::result::Result<Value, DecodeError> {
    if bytes.is_empty() {
        return Ok(Value::Null);
    }

    let mut sign = 1i8;
    let mut output_number = 0i64;

    for (i, b) in bytes.iter().enumerate() {
        // keep lower 4 bits of this byte
        let low_nibble = (b & 0b0000_1111) as i64;
        let high_nibble = ((b >> 4) & 0b0000_1111) as i64;
        if high_nibble >= 0 && high_nibble < 10 {
            output_number = output_number * 10 + high_nibble
        } else {
            // invalid nibble encountered - the format is wrong
            return Ok(Value::Null);
        }

        if picture.signed && i + 1 == bytes.len() {
            // The last nibble is a sign
            sign = match low_nibble {
                0x0C => 1, // +, signed
                0x0D => -1,
                0x0F => 1, // +, unsigned
                _ =>
                // invalid nibble encountered - the format is wrong
                {
                    return Ok(Value::Null);
                }
            }
        } else {
            if low_nibble >= 0 && low_nibble < 10 {
                output_number = output_number * 10 + low_nibble
            } else {
                // invalid nibble encountered - the format is wrong
                return Ok(Value::Null);
            }
        }
    }
    Ok(Value::Number(i64::from(sign) * output_number))
}
