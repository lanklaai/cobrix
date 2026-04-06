use crate::schema::{Field, Picture, Schema};
use crate::{CobolError, Result};

#[derive(Debug, Clone)]
pub struct ParserConfig {
    pub allow_comments: bool,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            allow_comments: true,
        }
    }
}

pub fn parse_copybook(copybook_text: &str, _cfg: &ParserConfig) -> Result<Schema> {
    let mut fields = Vec::new();

    for raw_line in copybook_text.lines() {
        let line = sanitize_line(raw_line);
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }

        let Ok(level) = parts[0].parse::<u8>() else {
            continue;
        };

        let name = parts[1].trim_end_matches('.').to_string();
        if name.eq_ignore_ascii_case("FILLER") {
            continue;
        }

        let occurs = parse_occurs(&parts).unwrap_or(1);
        let picture = extract_pic_clause(&line).map(parse_picture).transpose()?;

        fields.push(Field {
            level,
            name,
            picture,
            occurs,
        });
    }

    let leaf_count = fields.iter().filter(|f| f.is_leaf()).count();
    if leaf_count == 0 {
        return Err(CobolError::InvalidCopybook(
            "no leaf fields with PIC found".to_string(),
        ));
    }

    Ok(Schema {
        fields: fields.into_iter().filter(|f| f.is_leaf()).collect(),
    })
}

fn sanitize_line(line: &str) -> String {
    let mut trimmed = line.trim().to_string();
    if let Some(i) = trimmed.find("*") {
        if i == 0 {
            return String::new();
        }
    }
    if trimmed.starts_with("*") || trimmed.starts_with("/") {
        return String::new();
    }
    trimmed = trimmed.replace('\t', " ");
    trimmed
}

fn parse_occurs(parts: &[&str]) -> Option<usize> {
    parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("OCCURS"))
        .and_then(|i| parts.get(i + 1))
        .and_then(|n| n.trim_end_matches('.').parse::<usize>().ok())
}

fn extract_pic_clause(line: &str) -> Option<String> {
    let upper = line.to_ascii_uppercase();
    let idx = upper.find(" PIC ").or_else(|| upper.find(" PIC"))?;
    let suffix = &line[idx + 4..];
    let mut clause = suffix.trim();
    if let Some(i) = clause.find(" COMP") {
        clause = &clause[..i];
    }
    Some(clause.trim().trim_end_matches('.').to_string())
}

fn parse_picture(pic: String) -> Result<Picture> {
    let raw = pic.trim().to_ascii_uppercase();
    if raw.starts_with('X') {
        let len = parse_repeat_count(&raw, 'X')?;
        return Ok(Picture {
            raw,
            signed: false,
            digits_before: 0,
            digits_after: 0,
            alpha_len: Some(len),
        });
    }

    let signed = raw.starts_with('S');
    let body = if signed { &raw[1..] } else { &raw };
    let segments: Vec<&str> = body.split('V').collect();
    if segments.len() > 2 {
        return Err(CobolError::InvalidPicture(pic));
    }

    let digits_before = parse_numeric_digits(segments[0])?;
    let digits_after = if segments.len() == 2 {
        parse_numeric_digits(segments[1])?
    } else {
        0
    };

    if digits_before == 0 && digits_after == 0 {
        return Err(CobolError::InvalidPicture(pic));
    }

    Ok(Picture {
        raw,
        signed,
        digits_before,
        digits_after,
        alpha_len: None,
    })
}

fn parse_repeat_count(raw: &str, token: char) -> Result<usize> {
    if raw == token.to_string() {
        return Ok(1);
    }
    if let Some(start) = raw.find('(') {
        let end = raw
            .find(')')
            .ok_or_else(|| CobolError::InvalidPicture(raw.to_string()))?;
        let num = raw[start + 1..end]
            .parse::<usize>()
            .map_err(|_| CobolError::InvalidPicture(raw.to_string()))?;
        return Ok(num);
    }
    Ok(raw.chars().filter(|c| *c == token).count())
}

fn parse_numeric_digits(segment: &str) -> Result<usize> {
    let s = segment.trim().trim_matches('.');
    if s.is_empty() {
        return Ok(0);
    }
    if s.starts_with('9') {
        parse_repeat_count(s, '9')
    } else {
        Err(CobolError::InvalidPicture(segment.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_copybook() {
        let cb = "01 REC.\n 05 ID PIC 9(4).\n 05 NAME PIC X(10).";
        let schema = parse_copybook(cb, &ParserConfig::default()).expect("parse ok");
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fixed_record_len(), 14);
    }
}
